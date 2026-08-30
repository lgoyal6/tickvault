//! An advisory lock over an archive, held while a recorder is writing it.
//!
//! Recovery moves files. Run it against an archive that a recorder still has
//! open and it takes the in-progress file out from under the writer, which
//! stops the writer without stopping the feed: the sockets stay up, the frame
//! counters keep climbing, and nothing reaches disk. That failure was measured,
//! not imagined, and the command that caused it was `verify` with its default
//! flags.
//!
//! An OS lock rather than a pid file, because the kernel drops it when the
//! holder dies. A pid file has to guess whether the pid it names is the same
//! process that wrote it, and guesses wrong after a crash and a pid reuse.

use std::fs::{File, TryLockError};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Inside the archive, so moving the archive moves the lock with it.
const LOCK_FILE: &str = ".tickvault-writer.lock";

fn lock_path(archive: &Path) -> PathBuf {
    archive.join(LOCK_FILE)
}

fn open_lock_file(archive: &Path) -> Result<File> {
    // Never truncate: another process may be holding this very file, and the
    // point is to not disturb it.
    Ok(File::options()
        .create(true)
        .write(true)
        .truncate(false)
        .open(lock_path(archive))?)
}

/// Held for as long as this process is writing to an archive.
///
/// Dropping it releases the archive, as does the process exiting for any
/// reason.
#[derive(Debug)]
pub struct ArchiveLock {
    _file: File,
}

impl ArchiveLock {
    /// Claim an archive for writing, or fail saying it is already claimed.
    pub fn acquire(archive: &Path) -> Result<ArchiveLock> {
        std::fs::create_dir_all(archive)?;
        let file = open_lock_file(archive)?;
        match file.try_lock() {
            Ok(()) => Ok(ArchiveLock { _file: file }),
            Err(TryLockError::WouldBlock) => Err(Error::Other(format!(
                "another tickvault process is already writing {}",
                archive.display()
            ))),
            Err(TryLockError::Error(e)) => Err(Error::Io(e)),
        }
    }

    /// Whether some other process is writing this archive right now.
    ///
    /// False when the answer cannot be established, because a lock file that
    /// cannot be opened is not evidence of a recorder. The caller is choosing
    /// whether to mutate, and this only ever answers the easy half of that.
    pub fn is_busy(archive: &Path) -> bool {
        let Ok(file) = open_lock_file(archive) else {
            return false;
        };
        match file.try_lock() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            Err(TryLockError::WouldBlock) => true,
            Err(TryLockError::Error(_)) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unclaimed_archive_is_not_busy() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!ArchiveLock::is_busy(dir.path()));
    }

    #[test]
    fn a_claimed_archive_reads_as_busy_and_cannot_be_claimed_twice() {
        let dir = tempfile::tempdir().unwrap();
        let held = ArchiveLock::acquire(dir.path()).unwrap();
        assert!(ArchiveLock::is_busy(dir.path()));
        assert!(
            ArchiveLock::acquire(dir.path()).is_err(),
            "a second recorder must not get the same archive"
        );
        drop(held);
    }

    #[test]
    fn releasing_it_lets_the_next_process_in() {
        let dir = tempfile::tempdir().unwrap();
        drop(ArchiveLock::acquire(dir.path()).unwrap());
        assert!(!ArchiveLock::is_busy(dir.path()));
        ArchiveLock::acquire(dir.path()).expect("a released archive is claimable again");
    }
}
