//! The evaluator's command line.
//!
//! Every subcommand reads files that are already on disk. Nothing here can
//! connect to a venue, and there is no code path that places a real order.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tickvault_sim::eval::Control;
use tickvault_sim::feed::{SeqTamper, VenueFeed};
use tickvault_sim::manifest::Loaded;
use tickvault_sim::report;

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
    VerifyInputs {
        /// Print the hashes the manifest declares, in `shasum -c` format,
        /// instead of checking them. The gate script pipes this into shasum so
        /// the check is made by the tool rather than by us.
        #[arg(long)]
        print_declared: bool,
    },
    /// Replay every window of every venue and report what the sequence scheme
    /// concluded. Places nothing and trades nothing.
    Replay {
        /// Corrupt each window's identifiers first. The replay is expected to
        /// stop, and a non-zero exit means the enforcement works.
        #[arg(long)]
        tamper: bool,
    },
    /// Fit on earlier windows, run on later ones, and write the result out.
    Evaluate {
        /// A deliberate corruption. Only the gate script passes one, and every
        /// control run is expected to fail.
        #[arg(long, default_value = "none")]
        control: String,
        /// Where the JSON goes.
        #[arg(long)]
        out_json: PathBuf,
        /// Where every simulated fill goes.
        #[arg(long)]
        out_parquet: Option<PathBuf>,
        /// Where the table a person reads goes.
        #[arg(long)]
        out_report: Option<PathBuf>,
        /// A control run's JSON, to be judged and attached to this one. Repeat
        /// for each control.
        #[arg(long = "control-result")]
        control_results: Vec<PathBuf>,
    },
}

/// A control failed the way it was supposed to. Distinct from every other
/// failure so the gate script can tell "the control worked" from "the control
/// did not fire".
const CONTROL_FAILED_AS_REQUIRED: u8 = 2;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("tickvault-sim: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> tickvault_sim::SimResult<ExitCode> {
    let loaded = Loaded::open(&cli.manifest, &cli.repo_root)?;
    match &cli.command {
        Command::VerifyInputs { print_declared } => {
            if *print_declared {
                for (path, hash) in loaded.declared_hashes() {
                    println!("{hash}  {path}");
                }
                return Ok(ExitCode::SUCCESS);
            }
            let checked = loaded.verify_inputs()?;
            for (path, hash) in &checked {
                println!("{hash}  {path}");
            }
            println!(
                "{} input(s) match the manifest; manifest sha256 {}",
                checked.len(),
                loaded.sha256
            );
            Ok(ExitCode::SUCCESS)
        }
        Command::Replay { tamper } => replay(&loaded, *tamper).map(|()| ExitCode::SUCCESS),
        Command::Evaluate {
            control,
            out_json,
            out_parquet,
            out_report,
            control_results,
        } => evaluate(
            &loaded,
            control,
            out_json,
            out_parquet.as_deref(),
            out_report.as_deref(),
            control_results,
        ),
    }
}

/// Run the experiment and write it out.
///
/// The exit status carries a verdict of its own. A positive run exits zero even
/// when the candidate is rejected, because a negative result is a result. A
/// control run exits with a distinct code when it fails the way it must, so the
/// gate script can tell that apart from a control that quietly did nothing.
fn evaluate(
    loaded: &Loaded,
    control: &str,
    out_json: &std::path::Path,
    out_parquet: Option<&std::path::Path>,
    out_report: Option<&std::path::Path>,
    control_results: &[PathBuf],
) -> tickvault_sim::SimResult<ExitCode> {
    loaded.verify_inputs()?;
    let control = Control::parse(control)?;
    let evaluation = tickvault_sim::eval::evaluate(loaded, control)?;

    let mut controls = Vec::new();
    for path in control_results {
        controls.push(report::read_control(path, &evaluation)?);
    }
    report::write_json(out_json, &evaluation, &controls)?;
    if let Some(path) = out_parquet {
        report::write_trades(path, &evaluation.trades)?;
    }
    if let Some(path) = out_report {
        report::write_report(path, &evaluation, &controls)?;
    }

    println!(
        "control={} strategy_quality={} leakage_violations={} invariant_violations={} \
         truncated_windows={} comparable={}",
        evaluation.control,
        evaluation.gate.strategy_quality,
        evaluation.leakage_violations,
        evaluation.invariant_violations,
        evaluation.truncated_windows,
        evaluation.comparable
    );
    for aggregate in &evaluation.aggregates {
        println!(
            "  {:>18}  after_fee_pnl={:>12.4}  fills={:>5}  turnover={:>12.2}",
            aggregate.strategy,
            aggregate.after_fee_pnl_quote,
            aggregate.fills,
            aggregate.turnover_quote
        );
    }
    for outcome in &controls {
        println!(
            "  control {} failed_as_required={} : {}",
            outcome.control, outcome.failed_as_required, outcome.evidence
        );
        if !outcome.failed_as_required {
            return Err(tickvault_sim::SimError(format!(
                "the {} control did not fail the way it must, so it proves nothing",
                outcome.control
            )));
        }
    }

    match control {
        Control::None => {
            if evaluation.leakage_violations > 0 {
                return Err(tickvault_sim::SimError(format!(
                    "{} decision(s) leaked, which is a defect rather than a result",
                    evaluation.leakage_violations
                )));
            }
            if evaluation.invariant_violations > 0 {
                return Err(tickvault_sim::SimError(format!(
                    "{} held-out window(s) in gate scope stopped on a replay invariant",
                    evaluation.invariant_violations
                )));
            }
            Ok(ExitCode::SUCCESS)
        }
        Control::LeakFuture => {
            if evaluation.leakage_violations == 0 {
                return Err(tickvault_sim::SimError(
                    "the leak-future control injected a future value and the leakage detector \
                     said nothing, so the detector proves nothing"
                        .to_string(),
                ));
            }
            eprintln!(
                "tickvault-sim: leak-future control failed as required: {} decision(s) used a \
                 feature that was not yet visible",
                evaluation.leakage_violations
            );
            Ok(ExitCode::from(CONTROL_FAILED_AS_REQUIRED))
        }
        Control::NoCosts => {
            if evaluation.comparable {
                return Err(tickvault_sim::SimError(
                    "the no-costs control charged nothing and the result was still reported as \
                     comparable"
                        .to_string(),
                ));
            }
            eprintln!(
                "tickvault-sim: no-costs control failed as required: fees, rebates and slippage \
                 were zero, so this result is not comparable to a run that paid"
            );
            Ok(ExitCode::from(CONTROL_FAILED_AS_REQUIRED))
        }
        Control::ShuffleSeq => {
            if evaluation.truncated_windows == 0 {
                return Err(tickvault_sim::SimError(
                    "the shuffle-seq control corrupted the identifiers and no window stopped, so \
                     the replay invariants prove nothing"
                        .to_string(),
                ));
            }
            eprintln!(
                "tickvault-sim: shuffle-seq control failed as required: {} held-out window(s) \
                 stopped on a replay invariant",
                evaluation.truncated_windows
            );
            Ok(ExitCode::from(CONTROL_FAILED_AS_REQUIRED))
        }
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
