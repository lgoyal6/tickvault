//! Live websocket transport.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::types::VenueId;
use crate::venue::{FeedTransport, RawFrame};

/// A venue feed over TLS websocket.
pub struct WsTransport {
    venue: VenueId,
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    clock: Arc<dyn Clock>,
}

impl WsTransport {
    pub async fn connect(venue: VenueId, url: &str, clock: Arc<dyn Clock>) -> Result<Self> {
        let (stream, _response) = tokio_tungstenite::connect_async(url).await?;
        Ok(WsTransport {
            venue,
            stream,
            clock,
        })
    }
}

#[async_trait]
impl FeedTransport for WsTransport {
    async fn recv(&mut self) -> Result<Option<RawFrame>> {
        loop {
            match self.stream.next().await {
                // Stamp as soon as the bytes are in hand. Anything done before
                // this read shows up as venue latency that is really ours.
                Some(Ok(Message::Text(text))) => {
                    let stamp = self.clock.stamp();
                    return Ok(Some(RawFrame::new(
                        self.venue,
                        text.as_bytes().to_vec(),
                        stamp,
                    )));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    let stamp = self.clock.stamp();
                    return Ok(Some(RawFrame::new(self.venue, bytes.to_vec(), stamp)));
                }
                // tungstenite queues the pong itself; we only need to not treat
                // control frames as data.
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Err(e)) => return Err(Error::from(e)),
            }
        }
    }

    async fn send_text(&mut self, text: &str) -> Result<()> {
        self.stream.send(Message::Text(text.into())).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.stream.close(None).await?;
        Ok(())
    }
}
