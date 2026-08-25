//! Raw frame capture.
//!
//! Before anything is parsed, the exact bytes are written to disk with their
//! receipt stamps. Two reasons, and the second is the one that matters.
//!
//! First, a parser bug is recoverable if the raw bytes survive: reparse the
//! tape rather than lose the day. Second, and this is what the phase 1 gate
//! rests on, a tape can be replayed with messages deliberately removed. That
//! turns "does gap detection work" from an argument into a test.
//!
//! This is a deliberately plain writer. Phase 3 replaces it with the bounded
//! channel, the backpressure policy, and the crash-recovery guarantees; there
//! is nothing here that pretends to those yet.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::clock::Stamp;
use crate::error::Result;
use crate::types::VenueId;
use crate::venue::RawFrame;

/// One captured frame, as it is written to the tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedFrame {
    pub venue: VenueId,
    /// Monotonic receipt time, nanoseconds since the recorder started.
    pub t_mono: u64,
    /// Wall receipt time, nanoseconds since the Unix epoch.
    pub t_wall: i64,
    /// The frame verbatim.
    pub payload: String,
}

impl RecordedFrame {
    pub fn from_raw(frame: &RawFrame) -> Self {
        RecordedFrame {
            venue: frame.venue,
            t_mono: frame.stamp.mono_nanos,
            t_wall: frame.stamp.wall_nanos,
            payload: frame.text().into_owned(),
        }
    }

    pub fn to_raw(&self) -> RawFrame {
        RawFrame::new(
            self.venue,
            self.payload.clone().into_bytes(),
            Stamp {
                mono_nanos: self.t_mono,
                wall_nanos: self.t_wall,
            },
        )
    }
}

/// A whole recording, in memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawTape {
    pub frames: Vec<RecordedFrame>,
}

impl RawTape {
    pub fn new(frames: Vec<RecordedFrame>) -> Self {
        RawTape { frames }
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn push(&mut self, frame: RecordedFrame) {
        self.frames.push(frame);
    }

    /// Read a JSONL tape. Malformed lines fail the read rather than being
    /// skipped: a tape that quietly loses frames while being loaded would make
    /// the gap report measure the loader, not the feed.
    pub fn read_jsonl(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())?;
        let mut frames = Vec::new();
        for (n, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let frame: RecordedFrame = serde_json::from_str(line).map_err(|e| {
                crate::error::Error::Other(format!("{}:{}: {e}", path.as_ref().display(), n + 1))
            })?;
            frames.push(frame);
        }
        Ok(RawTape { frames })
    }

    /// Read a file of bare venue payloads, one per line.
    ///
    /// A captured tape carries its own receipt stamps; a file of payloads does
    /// not, so they are synthesised a millisecond apart from `first_wall`. That
    /// is a fiction, but a *stated* one, and it is fixed rather than taken from
    /// the clock so that replaying the same file twice produces the same
    /// archive. Anything reading the result gets ordering and spacing, not real
    /// latency, and the venue's own timestamps in the payloads are untouched.
    pub fn read_payloads(path: impl AsRef<Path>, venue: VenueId, first_wall: i64) -> Result<Self> {
        const STEP_NANOS: i64 = 1_000_000;
        let text = std::fs::read_to_string(path.as_ref())?;
        let frames = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
            .map(|(i, line)| RecordedFrame {
                venue,
                t_mono: (i as i64 * STEP_NANOS) as u64,
                t_wall: first_wall + i as i64 * STEP_NANOS,
                payload: line.to_string(),
            })
            .collect();
        Ok(RawTape { frames })
    }

    pub fn write_jsonl(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut out = String::new();
        for frame in &self.frames {
            out.push_str(&serde_json::to_string(frame)?);
            out.push('\n');
        }
        std::fs::write(path, out)?;
        Ok(())
    }

    /// A copy of this tape with the frames at `indices` removed.
    ///
    /// This is the gate's instrument: it simulates exactly what a lossy socket
    /// does, without needing one.
    pub fn without(&self, indices: &[usize]) -> RawTape {
        let drop: std::collections::BTreeSet<usize> = indices.iter().copied().collect();
        RawTape {
            frames: self
                .frames
                .iter()
                .enumerate()
                .filter(|(i, _)| !drop.contains(i))
                .map(|(_, f)| f.clone())
                .collect(),
        }
    }

    /// Indices whose payload contains `needle`. Used to target the drop at
    /// book messages rather than at subscription acks.
    pub fn indices_containing(&self, needle: &str) -> Vec<usize> {
        self.frames
            .iter()
            .enumerate()
            .filter(|(_, f)| f.payload.contains(needle))
            .map(|(i, _)| i)
            .collect()
    }
}

/// Appends frames to a tape file as they arrive.
pub struct RawRecorder {
    writer: BufWriter<tokio::fs::File>,
    frames: u64,
    bytes: u64,
}

impl RawRecorder {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::File::create(path).await?;
        Ok(RawRecorder {
            writer: BufWriter::new(file),
            frames: 0,
            bytes: 0,
        })
    }

    pub async fn record(&mut self, frame: &RawFrame) -> Result<()> {
        let line = serde_json::to_string(&RecordedFrame::from_raw(frame))?;
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.frames += 1;
        self.bytes += frame.payload.len() as u64;
        Ok(())
    }

    /// Flush buffered frames to the OS. Phase 3 adds the durability story; this
    /// only guarantees the bytes left our buffer.
    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        Ok(())
    }

    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// Payload bytes captured, excluding the JSONL envelope.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(n: u64, payload: &str) -> RecordedFrame {
        RecordedFrame {
            venue: VenueId::Kraken,
            t_mono: n,
            t_wall: 1_000 + n as i64,
            payload: payload.to_string(),
        }
    }

    fn tape() -> RawTape {
        RawTape::new(
            (0..5)
                .map(|i| frame(i, &format!("{{\"n\":{i}}}")))
                .collect(),
        )
    }

    #[test]
    fn raw_frames_survive_the_round_trip_byte_for_byte() {
        let original = frame(7, r#"{"a":"quoted \"inner\"","b":[1,2]}"#);
        let raw = original.to_raw();
        assert_eq!(RecordedFrame::from_raw(&raw), original);
        assert_eq!(raw.stamp.mono_nanos, 7);
    }

    #[test]
    fn a_tape_round_trips_through_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tape.jsonl");
        tape().write_jsonl(&path).unwrap();
        assert_eq!(RawTape::read_jsonl(&path).unwrap(), tape());
    }

    #[test]
    fn a_corrupt_line_fails_the_read_rather_than_vanishing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.jsonl");
        std::fs::write(
            &path,
            "{\"venue\":\"kraken\",\"t_mono\":1,\"t_wall\":1,\"payload\":\"x\"}\nnot json\n",
        )
        .unwrap();
        let err = RawTape::read_jsonl(&path).unwrap_err().to_string();
        assert!(err.contains(":2:"), "error should name the line: {err}");
    }

    #[test]
    fn dropping_frames_removes_exactly_those_indices() {
        let t = tape();
        let cut = t.without(&[1, 3]);
        assert_eq!(cut.len(), 3);
        let payloads: Vec<&str> = cut.frames.iter().map(|f| f.payload.as_str()).collect();
        assert_eq!(payloads, vec![r#"{"n":0}"#, r#"{"n":2}"#, r#"{"n":4}"#]);
        // The original is untouched, so one tape can seed many drop patterns.
        assert_eq!(t.len(), 5);
    }

    #[test]
    fn dropping_out_of_range_or_duplicate_indices_is_harmless() {
        let t = tape();
        assert_eq!(t.without(&[99]).len(), 5);
        assert_eq!(t.without(&[2, 2, 2]).len(), 4);
    }

    #[test]
    fn indices_containing_finds_the_frames_worth_dropping() {
        let t = RawTape::new(vec![
            frame(0, r#"{"type":"subscriptions"}"#),
            frame(1, r#"{"type":"snapshot"}"#),
            frame(2, r#"{"type":"update"}"#),
            frame(3, r#"{"type":"update"}"#),
        ]);
        assert_eq!(t.indices_containing("update"), vec![2, 3]);
    }

    #[tokio::test]
    async fn the_recorder_writes_a_readable_tape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("out.jsonl");
        let mut rec = RawRecorder::create(&path).await.unwrap();
        for f in &tape().frames {
            rec.record(&f.to_raw()).await.unwrap();
        }
        rec.flush().await.unwrap();
        assert_eq!(rec.frames(), 5);
        assert_eq!(RawTape::read_jsonl(&path).unwrap(), tape());
    }
}
