//! The evaluator's command line.
//!
//! Every subcommand reads files that are already on disk. Nothing here can
//! connect to a venue, and there is no code path that places a real order.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
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
    }
}
