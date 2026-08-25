//! Throwaway capture: dump raw frames, one JSON payload per line, skipping any
//! frame larger than a cap so a multi-megabyte snapshot does not land in a
//! fixture. Usage: capture <url> <subscribe-json> <frames> <max-bytes>
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = std::env::args().collect();
    let n: usize = a[3].parse()?;
    let cap: usize = a[4].parse()?;
    let (mut ws, _) = tokio_tungstenite::connect_async(a[1].as_str()).await?;
    // "-" means the stream is selected by URL and wants no subscribe frame.
    if a[2] != "-" {
        ws.send(Message::Text(a[2].as_str().into())).await?;
    }
    let mut kept = 0;
    while kept < n {
        match tokio::time::timeout(std::time::Duration::from_secs(15), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let s = t.to_string();
                if s.len() <= cap {
                    println!("{s}");
                    kept += 1;
                } else {
                    eprintln!("skipped {} byte frame", s.len());
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    Ok(())
}
