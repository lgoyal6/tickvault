//! Immutable, checksum-bound selections of an archive. File integrity is
//! distinct from market-data verifiability; a hash does not prove feed loss was checked.
use crate::{Result, digest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};
use tickvault::{
    store::{
        manifest::{FileRecord, TruncationRecord},
        reader::{ArchiveReader, inspect},
        rows::RowStream,
    },
    types::{Symbol, VenueId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAttestation {
    pub adapter_version: String,
    pub validator_version: String,
    pub can_detect_loss: bool,
    pub scope: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetFile {
    pub record: FileRecord,
    pub sha256: String,
    pub bytes: u64,
    pub price_scale: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub id: String,
    pub version: u32,
    pub schema_version: u32,
    pub files: Vec<DatasetFile>,
    pub truncations: Vec<TruncationRecord>,
    pub clock: String,
    pub known_blind_spots: Vec<String>,
    pub creation_command: String,
    pub manifest_sha256: String,
    /// Absent for legacy captures. Absence must stay unverifiable.
    #[serde(default)]
    pub capability: Option<CapabilityAttestation>,
}
fn safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}
fn contained(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path
        .components()
        .any(|c| !matches!(c, Component::Normal(_)))
        || relative.is_empty()
    {
        return Err("unsafe dataset path".into());
    }
    let joined = root.join(path);
    // Reject symlinks at every component, including intermediate directories.
    let mut current = root.to_path_buf();
    for c in path.components() {
        current.push(c);
        if fs::symlink_metadata(&current)?.file_type().is_symlink() {
            return Err("dataset symlinks are not allowed".into());
        }
    }
    Ok(joined)
}
fn file_digest(path: &Path) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        bytes += n as u64;
    }
    Ok((
        hash.finalize().iter().map(|b| format!("{b:02x}")).collect(),
        bytes,
    ))
}
impl DatasetManifest {
    pub fn identity(&self) -> Result<String> {
        let mut copy = self.clone();
        copy.manifest_sha256.clear();
        Ok(digest(&serde_json::to_vec(&copy)?))
    }
    pub fn capture(root: &Path, id: &str, version: u32, creation_command: String) -> Result<Self> {
        Self::capture_with_capability(root, id, version, creation_command, None)
    }

    /// Capture a verified archive and bind the operator-supplied adapter
    /// attestation into the immutable identity. The archive format deliberately
    /// does not guess which process wrote it.
    pub fn capture_with_capability(
        root: &Path,
        id: &str,
        version: u32,
        creation_command: String,
        capability: Option<CapabilityAttestation>,
    ) -> Result<Self> {
        if !safe_id(id) || version == 0 {
            return Err("invalid dataset id or version".into());
        }
        let archive = ArchiveReader::open(root)?;
        let report = archive.verify();
        if !report.is_clean() {
            return Err(format!("archive verification failed: {report}").into());
        }
        let mut files = Vec::new();
        for record in archive.files() {
            let path = contained(root, &record.path)?;
            let summary = inspect(&path)?;
            let (sha256, bytes) = file_digest(&path)?;
            files.push(DatasetFile {
                record: record.clone(),
                sha256,
                bytes,
                price_scale: summary.price_scale,
            });
        }
        files.sort_by(|a, b| a.record.path.cmp(&b.record.path));
        if files.is_empty() {
            return Err("cannot publish an empty dataset".into());
        }
        let mut value = Self {
            id: id.into(),
            version,
            schema_version: 1,
            files,
            truncations: archive.manifest().truncations().to_vec(),
            clock: "signed UTC receipt nanoseconds; venue timestamps are not availability times"
                .into(),
            known_blind_spots: if capability.is_some() {
                vec!["Checksums detect changed bytes; they do not authenticate the dataset publisher.".into()]
            } else {
                vec!["Archives do not persist the capture adapter version or its loss-check capability. Feed verifiability must remain unknown until independently attested.".into(), "Checksums detect changed bytes; they do not authenticate the dataset publisher.".into()]
            },
            creation_command,
            manifest_sha256: String::new(),
            capability,
        };
        value.manifest_sha256 = value.identity()?;
        Ok(value)
    }
    pub fn publish(&self, directory: &Path) -> Result<PathBuf> {
        if !safe_id(&self.id) || self.version == 0 || self.identity()? != self.manifest_sha256 {
            return Err("invalid manifest identity".into());
        }
        fs::create_dir_all(directory)?;
        let path = directory.join(format!("{}@v{}.json", self.id, self.version));
        let bytes = serde_json::to_vec(self)?;
        if path.exists() {
            if fs::read(&path)? == bytes {
                return Ok(path);
            }
            return Err("dataset version already published with different content".into());
        }
        // A hard link publishes a fully synced inode without replacing a racing
        // publisher's version. A plain rename could overwrite that version.
        let temporary = tempfile::NamedTempFile::new_in(directory)?;
        temporary.as_file().write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        match fs::hard_link(temporary.path(), &path) {
            Ok(()) => {
                File::open(directory)?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(&path)? != bytes {
                    return Err("concurrent dataset publication conflict".into());
                }
            }
            Err(e) => return Err(e.into()),
        }
        Ok(path)
    }
}
/// Owns a private copy of the selected files, so a live recorder, retention or
/// compaction cannot change the bytes between verification and lazy reads.
#[derive(Debug)]
pub struct VerifiedDataset {
    manifest: DatasetManifest,
    snapshot: tempfile::TempDir,
}
impl VerifiedDataset {
    pub fn open(root: &Path, manifest_path: &Path) -> Result<Self> {
        let manifest: DatasetManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
        if !safe_id(&manifest.id)
            || manifest.version == 0
            || manifest.schema_version != 1
            || manifest.files.is_empty()
            || manifest.identity()? != manifest.manifest_sha256
        {
            return Err("invalid dataset manifest".into());
        }
        let archive = ArchiveReader::open(root)?;
        let snapshot = tempfile::tempdir()?;
        let mut paths = BTreeSet::new();
        for entry in &manifest.files {
            if !paths.insert(&entry.record.path) {
                return Err("duplicate dataset file".into());
            }
            if !archive.files().contains(&entry.record) {
                return Err("dataset file retired or archive metadata changed".into());
            }
            let source = contained(root, &entry.record.path)?;
            let destination = snapshot.path().join(&entry.record.path);
            fs::create_dir_all(destination.parent().ok_or("missing file parent")?)?;
            fs::copy(source, &destination)?;
            let (hash, bytes) = file_digest(&destination)?;
            if hash != entry.sha256 || bytes != entry.bytes {
                return Err("dataset file checksum mismatch".into());
            }
            let summary = inspect(&destination)?;
            if summary.rows != entry.record.rows
                || summary.venue != Some(entry.record.venue)
                || summary.symbol.as_ref() != Some(&entry.record.symbol)
                || summary.price_scale != entry.price_scale
                || summary.first_recv_wall != entry.record.first_recv_wall
                || summary.last_recv_wall != entry.record.last_recv_wall
                || summary.book_level != entry.record.book_level
            {
                return Err("dataset file metadata mismatch".into());
            }
        }
        Ok(Self { manifest, snapshot })
    }
    pub fn manifest(&self) -> &DatasetManifest {
        &self.manifest
    }
    pub fn rows(&self, venue: VenueId, symbol: &Symbol, until_ns: i64) -> RowStream {
        let mut files: Vec<_> = self
            .manifest
            .files
            .iter()
            .filter(|f| f.record.venue == venue && &f.record.symbol == symbol)
            .map(|f| f.record.clone())
            .collect();
        files.sort_by(|a, b| (a.first_recv_wall, &a.path).cmp(&(b.first_recv_wall, &b.path)));
        RowStream::new(self.snapshot.path(), files, until_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/data/kraken")
    }
    #[test]
    fn frozen_reads_survive_source_changes_and_new_opens_reject_them() {
        let root = tempfile::tempdir().unwrap();
        let source = fixture();
        fs::copy(
            source.join("_manifest.jsonl"),
            root.path().join("_manifest.jsonl"),
        )
        .unwrap();
        let archive = ArchiveReader::open(&source).unwrap();
        for record in archive.files() {
            let dest = root.path().join(&record.path);
            fs::create_dir_all(dest.parent().unwrap()).unwrap();
            fs::copy(source.join(&record.path), dest).unwrap();
        }
        let manifest =
            DatasetManifest::capture(root.path(), "kraken", 1, "fixture".into()).unwrap();
        let publication = tempfile::tempdir().unwrap();
        let path = manifest.publish(publication.path()).unwrap();
        let dataset = VerifiedDataset::open(root.path(), &path).unwrap();
        let record = &manifest.files[0].record;
        let before: Vec<_> = dataset
            .rows(record.venue, &record.symbol, i64::MAX)
            .collect::<tickvault::error::Result<_>>()
            .unwrap();
        fs::write(root.path().join(&record.path), b"corruption").unwrap();
        let after: Vec<_> = dataset
            .rows(record.venue, &record.symbol, i64::MAX)
            .collect::<tickvault::error::Result<_>>()
            .unwrap();
        assert_eq!(before, after);
        assert!(!after.is_empty());
        assert!(VerifiedDataset::open(root.path(), &path).is_err());
    }
    #[test]
    fn publication_is_idempotent_but_refuses_rewriting_a_version() {
        let manifest = DatasetManifest::capture(&fixture(), "kraken", 1, "fixture".into()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let first = manifest.publish(dir.path()).unwrap();
        assert_eq!(first, manifest.publish(dir.path()).unwrap());
        let mut changed = manifest.clone();
        changed.creation_command = "different".into();
        changed.manifest_sha256 = changed.identity().unwrap();
        assert!(changed.publish(dir.path()).is_err());
        changed.version = 2;
        changed.manifest_sha256 = changed.identity().unwrap();
        assert!(changed.publish(dir.path()).is_ok());
    }
    #[test]
    fn edits_and_traversal_are_rejected() {
        let mut manifest =
            DatasetManifest::capture(&fixture(), "kraken", 1, "fixture".into()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = manifest.publish(dir.path()).unwrap();
        manifest.id = "changed".into();
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(VerifiedDataset::open(&fixture(), &path).is_err());
        manifest.files[0].record.path = "../outside.parquet".into();
        manifest.manifest_sha256 = manifest.identity().unwrap();
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(VerifiedDataset::open(&fixture(), &path).is_err());
    }
}
