//! Replaying a recorded tape as if it were a live feed.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::clock::Clock;
use crate::error::Result;
use crate::recorder::RawTape;
use crate::venue::{FeedTransport, RawFrame};

/// Frames the replay was asked to send upstream. A live venue swallows these;
/// here they are kept so a test can assert what the subscribe step produced.
pub type SentFrames = Arc<Mutex<Vec<String>>>;

/// Plays a [`RawTape`] back through the ordinary ingest path.
///
/// Frames carry their originally recorded stamps by default, so a replay of the
/// same tape produces the same timings and the same gap report every time.
pub struct ReplayTransport {
    frames: std::vec::IntoIter<crate::recorder::RecordedFrame>,
    sent: SentFrames,
    restamp: Option<Arc<dyn Clock>>,
}

impl ReplayTransport {
    pub fn new(tape: RawTape) -> Self {
        ReplayTransport {
            frames: tape.frames.into_iter(),
            sent: Arc::new(Mutex::new(Vec::new())),
            restamp: None,
        }
    }

    /// Stamp frames from `clock` instead of using their recorded times. Useful
    /// for measuring how fast a replay runs; wrong for anything that reports
    /// the original recording's latencies.
    pub fn restamped(mut self, clock: Arc<dyn Clock>) -> Self {
        self.restamp = Some(clock);
        self
    }

    /// Handle to the frames this transport is asked to send. Take it before
    /// boxing the transport.
    pub fn sent_handle(&self) -> SentFrames {
        Arc::clone(&self.sent)
    }
}

#[async_trait]
impl FeedTransport for ReplayTransport {
    async fn recv(&mut self) -> Result<Option<RawFrame>> {
        Ok(self.frames.next().map(|recorded| {
            let mut raw = recorded.to_raw();
            if let Some(clock) = &self.restamp {
                raw.stamp = clock.stamp();
            }
            raw
        }))
    }

    async fn send_text(&mut self, text: &str) -> Result<()> {
        self.sent
            .lock()
            .expect("sent frames lock")
            .push(text.to_string());
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::recorder::RecordedFrame;
    use crate::types::VenueId;

    fn tape() -> RawTape {
        RawTape::new(
            (0..3)
                .map(|i| RecordedFrame {
                    venue: VenueId::Kraken,
                    t_mono: i * 100,
                    t_wall: 5_000 + i as i64 * 100,
                    payload: format!("frame-{i}"),
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn replay_preserves_recorded_stamps_and_then_ends() {
        let mut t = ReplayTransport::new(tape());
        for i in 0..3u64 {
            let frame = t.recv().await.unwrap().expect("frame");
            assert_eq!(frame.stamp.mono_nanos, i * 100);
            assert_eq!(frame.text(), format!("frame-{i}"));
        }
        assert!(t.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn restamping_overrides_recorded_times() {
        let clock = Arc::new(ManualClock::new(0, 7));
        let mut t = ReplayTransport::new(tape()).restamped(clock);
        assert_eq!(t.recv().await.unwrap().unwrap().stamp.mono_nanos, 0);
        assert_eq!(t.recv().await.unwrap().unwrap().stamp.mono_nanos, 7);
    }

    #[tokio::test]
    async fn subscribe_frames_are_observable() {
        let mut t = ReplayTransport::new(tape());
        let sent = t.sent_handle();
        t.send_text("{\"method\":\"subscribe\"}").await.unwrap();
        assert_eq!(sent.lock().unwrap().len(), 1);
    }
}
