//! The evaluator's command line.
//!
//! Every subcommand reads files that are already on disk. Nothing here can
//! connect to a venue, and there is no code path that places a real order.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tickvault_sim::feed::{SeqTamper, VenueFeed};
use tickvault_sim::manifest::Loaded;

#[derive(Parser)]
#[command(
    name = "tickvault-sim",
    about = "Replay a recorded tickvault archive with simulated execution",
    long_about = "Replays recorded archive data on one host with simulated execution. No venue is \
                  contacted, no order is placed anywhere, and no money moves."
)]
struct Cli {
    /// The frozen experiment manifest.
    #[arg(long, default_value = "sim/manifest.json")]
    manifest: PathBuf,
    /// Repository root the manifest's relative paths resolve against.
    #[arg(long, default_value = ".")]
    repo_root: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check every input the manifest names against the bytes on disk.
    VerifyInputs,
    /// Replay every window of every venue and report what the sequence scheme
    /// concluded. Places nothing and trades nothing.
    Replay {
        /// Corrupt each window's identifiers first. The replay is expected to
        /// stop, and a non-zero exit means the enforcement works.
        #[arg(long)]
        tamper: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("tickvault-sim: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> tickvault_sim::SimResult<()> {
    let loaded = Loaded::open(&cli.manifest, &cli.repo_root)?;
    match cli.command {
        Command::VerifyInputs => {
            let checked = loaded.verify_inputs()?;
            for (path, hash) in &checked {
                println!("{hash}  {path}");
            }
            println!(
                "{} input(s) match the manifest; manifest sha256 {}",
                checked.len(),
                loaded.sha256
            );
            Ok(())
        }
        Command::Replay { tamper } => replay(&loaded, tamper),
    }
}

/// Walk every window of every venue, enforcing the venue's own scheme.
fn replay(loaded: &Loaded, tamper: bool) -> tickvault_sim::SimResult<()> {
    loaded.verify_inputs()?;
    let precision = loaded.manifest.kraken_checksum_precision;
    let mut stopped = 0usize;
    for input in &loaded.manifest.inputs {
        let feed = VenueFeed::load(&loaded.repo_root, input)?;
        let windows = loaded.windows_for(&input.venue)?;
        let states = feed.window_states(windows, precision)?;
        println!(
            "{} {} scheme={} verifiable={} messages={} tick={}",
            feed.venue,
            feed.symbol.as_str(),
            feed.scheme.as_str(),
            feed.verifiable,
            feed.messages.len(),
            feed.observed_tick.to_decimal_string(9)
        );
        for (window, start) in windows.iter().zip(states) {
            let (from, to) = feed.range_of(window);
            let messages = &feed.messages[from..to];
            let mutation = if tamper {
                SeqTamper::for_window(messages.len())
            } else {
                SeqTamper::default()
            };
            let mut state = start;
            let before = state.enforcer.counts;
            let mut applied = 0usize;
            let mut violation = None;
            for (local, message) in messages.iter().enumerate() {
                if mutation.duplicate_at == Some(local)
                    && let Err(v) = state.apply(message, mutation.ident_at(local, messages))
                {
                    violation = Some(v);
                    break;
                }
                if let Err(v) = state.apply(message, mutation.ident_at(local, messages)) {
                    violation = Some(v);
                    break;
                }
                applied += 1;
            }
            match &violation {
                Some(v) => {
                    stopped += 1;
                    println!(
                        "  window {} truncated after {applied}/{} messages: {v}",
                        window.index,
                        messages.len()
                    );
                }
                None => println!(
                    "  window {} clean: {} messages, in_order={} unverifiable={} forward_skips={}",
                    window.index,
                    messages.len(),
                    state.enforcer.counts.in_order - before.in_order,
                    state.enforcer.counts.unverifiable - before.unverifiable,
                    state.enforcer.counts.counter_forward_skips - before.counter_forward_skips
                ),
            }
        }
    }
    if tamper {
        if stopped == 0 {
            return Err(tickvault_sim::SimError(
                "the tamper control changed the identifiers and no window stopped, so the \
                 enforcement proves nothing"
                    .to_string(),
            ));
        }
        return Err(tickvault_sim::SimError(format!(
            "tamper control: {stopped} window(s) stopped as they must"
        )));
    }
    if stopped > 0 {
        return Err(tickvault_sim::SimError(format!(
            "{stopped} window(s) stopped on a replay invariant"
        )));
    }
    Ok(())
}
