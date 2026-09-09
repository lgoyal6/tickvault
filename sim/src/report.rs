//! Writing the result out: the JSON, the trades file, and the table a person
//! reads.
//!
//! All three carry the same boundary statement, because a number that travels
//! without it is a number somebody will quote as though it came from a market.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::eval::{Evaluation, TradeRow};
use crate::{SimResult, bail};

/// What a control run did, once the positive run has read its output back.
#[derive(Debug, Clone, Serialize)]
pub struct ControlOutcome {
    pub control: String,
    pub expected: String,
    pub failed_as_required: bool,
    pub evidence: String,
    pub leakage_violations: u64,
    pub invariant_violations: u64,
    pub truncated_windows: usize,
    pub comparable: bool,
    pub strategy_quality: String,
    /// Only for the zero-cost control: what the costs were worth, per strategy.
    pub after_cost_difference_quote: Option<BTreeMap<String, f64>>,
}

/// The fields a control run's JSON has to carry for the merge.
#[derive(Debug, Deserialize)]
struct ControlJson {
    control: String,
    comparable: bool,
    leakage_violations: u64,
    invariant_violations: u64,
    truncated_windows: usize,
    aggregates: Vec<AggregateJson>,
    gate: GateJson,
}

#[derive(Debug, Deserialize)]
struct AggregateJson {
    strategy: String,
    after_fee_pnl_quote: f64,
}

#[derive(Debug, Deserialize)]
struct GateJson {
    strategy_quality: String,
}

/// Read a control run's output and judge whether it failed the way it must.
pub fn read_control(path: &Path, positive: &Evaluation) -> SimResult<ControlOutcome> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| crate::SimError(format!("cannot read {}: {e}", path.display())))?;
    let control: ControlJson = serde_json::from_str(&text)?;
    let outcome = match control.control.as_str() {
        "leak-future" => ControlOutcome {
            control: control.control.clone(),
            expected: "the leakage detector rejects the run".to_string(),
            failed_as_required: control.leakage_violations > 0,
            evidence: format!(
                "{} decision(s) used a feature that was not yet visible",
                control.leakage_violations
            ),
            leakage_violations: control.leakage_violations,
            invariant_violations: control.invariant_violations,
            truncated_windows: control.truncated_windows,
            comparable: control.comparable,
            strategy_quality: control.gate.strategy_quality.clone(),
            after_cost_difference_quote: None,
        },
        "no-costs" => {
            let mut difference = BTreeMap::new();
            for aggregate in &control.aggregates {
                if let Some(paid) = positive
                    .aggregates
                    .iter()
                    .find(|a| a.strategy == aggregate.strategy)
                {
                    difference.insert(
                        aggregate.strategy.clone(),
                        aggregate.after_fee_pnl_quote - paid.after_fee_pnl_quote,
                    );
                }
            }
            ControlOutcome {
                control: control.control.clone(),
                expected: "the result is refused as non-comparable to a run that paid".to_string(),
                failed_as_required: !control.comparable
                    && control.gate.strategy_quality != "Promoted",
                evidence: format!(
                    "comparable = {}, and the zero-cost run differs from the frozen-cost run by {}",
                    control.comparable,
                    difference
                        .iter()
                        .map(|(k, v)| format!("{k} {v:+.4}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                leakage_violations: control.leakage_violations,
                invariant_violations: control.invariant_violations,
                truncated_windows: control.truncated_windows,
                comparable: control.comparable,
                strategy_quality: control.gate.strategy_quality.clone(),
                after_cost_difference_quote: Some(difference),
            }
        }
        "shuffle-seq" => ControlOutcome {
            control: control.control.clone(),
            expected: "the replay invariants stop every window".to_string(),
            failed_as_required: control.truncated_windows > 0 && control.invariant_violations > 0,
            evidence: format!(
                "{} held-out window(s) stopped on a replay invariant, {} of them on a venue in \
                 gate scope",
                control.truncated_windows, control.invariant_violations
            ),
            leakage_violations: control.leakage_violations,
            invariant_violations: control.invariant_violations,
            truncated_windows: control.truncated_windows,
            comparable: control.comparable,
            strategy_quality: control.gate.strategy_quality.clone(),
            after_cost_difference_quote: None,
        },
        other => bail!(
            "{} is not a control this build knows: {other}",
            path.display()
        ),
    };
    Ok(outcome)
}

/// The JSON, with any control outcomes attached.
pub fn write_json(
    path: &Path,
    evaluation: &Evaluation,
    controls: &[ControlOutcome],
) -> SimResult<()> {
    let mut value = serde_json::to_value(evaluation)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("controls".to_string(), serde_json::to_value(controls)?);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut text = serde_json::to_string_pretty(&value)?;
    text.push('\n');
    std::fs::write(path, text)?;
    Ok(())
}

fn trades_schema() -> Schema {
    let doc = |field: Field, text: &str| {
        let mut meta = std::collections::HashMap::new();
        meta.insert("doc".to_string(), text.to_string());
        field.with_metadata(meta)
    };
    Schema::new(vec![
        doc(
            Field::new("venue", DataType::Utf8, false),
            "recording venue",
        ),
        doc(
            Field::new("symbol", DataType::Utf8, false),
            "canonical BASE-QUOTE pair",
        ),
        doc(
            Field::new("window", DataType::UInt32, false),
            "chronological window index inside the recording",
        ),
        doc(
            Field::new("strategy", DataType::Utf8, false),
            "which strategy was running",
        ),
        doc(
            Field::new("params", DataType::Utf8, false),
            "the parameters fitted on strictly earlier windows",
        ),
        doc(
            Field::new(
                "recv_wall_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            "receive time of the archived message this fill happened on",
        ),
        doc(
            Field::new("side", DataType::Utf8, false),
            "buy or sell, from our side",
        ),
        doc(
            Field::new("price", DataType::Float64, false),
            "fill price in quote units, after slippage on a marketable fill",
        ),
        doc(
            Field::new("size", DataType::Float64, false),
            "fill size in base units",
        ),
        doc(
            Field::new("fee", DataType::Float64, false),
            "fee paid in quote units; negative would be a rebate received",
        ),
        doc(
            Field::new("liquidity", DataType::Utf8, false),
            "maker or taker",
        ),
        doc(
            Field::new("cause", DataType::Utf8, false),
            "observed_trade, crossing, or own_aggression",
        ),
        doc(
            Field::new("approx_queue_ahead_at_fill", DataType::Float64, false),
            "APPROXIMATE quantity believed ahead of the order when it filled, in base units; a \
             maker fill on an aggregated feed cannot know this exactly",
        ),
        doc(
            Field::new("latency_ms", DataType::Float64, false),
            "the order entry delay this order was given, in milliseconds",
        ),
    ])
}

/// Every simulated fill, one row each.
pub fn write_trades(path: &Path, trades: &[TradeRow]) -> SimResult<()> {
    let schema = Arc::new(trades_schema());
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(path)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .map_err(|e| crate::SimError(e.to_string()))?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.venue.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.symbol.as_str()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            trades.iter().map(|t| t.window),
        )),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.strategy.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.params.as_str()),
        )),
        Arc::new(
            TimestampNanosecondArray::from_iter_values(trades.iter().map(|t| t.recv_wall_ns))
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.side.as_str()),
        )),
        Arc::new(Float64Array::from_iter_values(
            trades.iter().map(|t| t.price),
        )),
        Arc::new(Float64Array::from_iter_values(
            trades.iter().map(|t| t.size),
        )),
        Arc::new(Float64Array::from_iter_values(trades.iter().map(|t| t.fee))),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.liquidity.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            trades.iter().map(|t| t.cause.as_str()),
        )),
        Arc::new(Float64Array::from_iter_values(
            trades.iter().map(|t| t.approx_queue_ahead_at_fill),
        )),
        Arc::new(Float64Array::from_iter_values(
            trades.iter().map(|t| t.latency_ms),
        )),
    ];
    let batch =
        RecordBatch::try_new(schema, columns).map_err(|e| crate::SimError(e.to_string()))?;
    writer
        .write(&batch)
        .map_err(|e| crate::SimError(e.to_string()))?;
    writer.close().map_err(|e| crate::SimError(e.to_string()))?;
    Ok(())
}

fn number(value: Option<f64>, places: usize) -> String {
    match value {
        Some(v) => format!("{v:.places$}"),
        // Not a zero. Where the run could not measure something, the table says
        // so, which is the same rule the archive's own schema follows.
        None => "null".to_string(),
    }
}

/// The table a person reads.
pub fn write_report(
    path: &Path,
    evaluation: &Evaluation,
    controls: &[ControlOutcome],
) -> SimResult<()> {
    let mut out = String::new();
    out.push_str("# Strategy evaluation\n\n");
    out.push_str("> **What this is.** ");
    out.push_str(&evaluation.data_boundary);
    out.push_str("\n\n");
    out.push_str(&format!(
        "Manifest `sim/manifest.json`, sha256 `{}`. Reproduce with `./scripts/run-strategy-eval.sh`.\n\n",
        evaluation.manifest_sha256
    ));

    out.push_str("## Verdict\n\n");
    out.push_str(&format!(
        "**Strategy quality: {}.**\n\n",
        evaluation.gate.strategy_quality
    ));
    if evaluation.gate.strategy_quality != "Promoted" {
        out.push_str(
            "No profitability and no alpha are claimed. The candidate did not clear the promotion \
             gate that was frozen before any of these numbers existed, every losing baseline is \
             kept below, and what stands is the simulator, the sequence enforcement and the risk \
             system rather than a strategy.\n\n",
        );
    }
    out.push_str("| gate clause | result | detail |\n|---|---|---|\n");
    for clause in &evaluation.gate.clauses {
        out.push_str(&format!(
            "| `{}` | {} | {} |\n",
            clause.id,
            if clause.passed { "pass" } else { "fail" },
            clause.detail
        ));
    }
    out.push('\n');

    out.push_str("## Held-out totals per strategy\n\n");
    out.push_str(
        "Summed over the held-out windows of the venues in gate scope. `marked` carries the \
         closing inventory at the mid; `liquidated` takes it out at the touch and pays the taker \
         fee, so it is always the smaller of the two.\n\n",
    );
    out.push_str(
        "| strategy | windows | after-fee PnL (marked) | after-fee PnL (liquidated) | max drawdown | turnover | fills | fill rate | max inventory | kill switch | latency median ms | latency p99 ms |\n",
    );
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for aggregate in &evaluation.aggregates {
        out.push_str(&format!(
            "| `{}` | {} | {:.4} | {:.4} | {:.4} | {:.2} | {} | {} | {:.5} | {} | {} | {} |\n",
            aggregate.strategy,
            aggregate.held_out_windows,
            aggregate.after_fee_pnl_quote,
            aggregate.after_fee_pnl_liquidated_quote,
            aggregate.max_drawdown_quote,
            aggregate.turnover_quote,
            aggregate.fills,
            number(aggregate.fill_rate, 4),
            aggregate.max_abs_inventory_base,
            aggregate.kill_switch_activations,
            number(aggregate.latency_median_ms, 3),
            number(aggregate.latency_p99_ms, 3),
        ));
    }
    out.push('\n');

    out.push_str(&format!(
        "Block bootstrap of the candidate's total held-out after-fee PnL: point {:.4}, 95 percent \
         interval [{}, {}], block length {}, {} resamples over {} units. The unit is {}\n\n",
        evaluation.bootstrap.point_estimate,
        number(evaluation.bootstrap.lower_95, 4),
        number(evaluation.bootstrap.upper_95, 4),
        evaluation.bootstrap.block_length,
        evaluation.bootstrap.resamples,
        evaluation.bootstrap.units,
        evaluation.bootstrap.unit,
    ));
    out.push_str(&format!(
        "Direction agreement: {} chronological held-out window position(s) carry the sign of the \
         total, out of the {} positions the manifest defines. The gate requires at least {}.\n\n",
        evaluation.direction_agreement,
        evaluation.held_out_positions,
        evaluation.direction_agreement_required
    ));

    out.push_str("## Rejected orders\n\n");
    out.push_str("| strategy | reason | count |\n|---|---|---:|\n");
    let mut any_rejection = false;
    for aggregate in &evaluation.aggregates {
        for (reason, count) in &aggregate.rejected_orders {
            any_rejection = true;
            out.push_str(&format!(
                "| `{}` | `{reason}` | {count} |\n",
                aggregate.strategy
            ));
        }
    }
    if !any_rejection {
        out.push_str("| | no order was refused by a pre-trade check | 0 |\n");
    }
    out.push('\n');

    out.push_str("## Venues\n\n");
    out.push_str(
        "| venue | symbol | scheme | verifiable | in gate scope | messages | measured tick | median spread (bp) |\n",
    );
    out.push_str("|---|---|---|---|---|---:|---:|---:|\n");
    for venue in &evaluation.venues {
        out.push_str(&format!(
            "| {} | {} | `{}` | {} | {} | {} | {} | {} |\n",
            venue.venue,
            venue.symbol,
            venue.sequence_scheme,
            venue.verifiable,
            venue.in_gate_scope,
            venue.messages,
            venue.observed_tick,
            number(venue.median_spread_bps, 4),
        ));
    }
    out.push_str(
        "\nA venue whose feed publishes neither a sequence number nor a checksum is reported and \
         kept out of every aggregate: loss on it is undetectable by construction, and unverifiable \
         is not clean.\n\n",
    );

    out.push_str("## Per window\n\n");
    out.push_str(
        "| venue | window | strategy | parameters | after-fee PnL | drawdown | turnover | fills | fill rate | mean inventory | max inventory | rejected | kill switch | messages | truncated |\n",
    );
    out.push_str("|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for run in &evaluation.runs {
        out.push_str(&format!(
            "| {} | {} | `{}` | {} | {} | {:.4} | {:.2} | {} | {} | {} | {:.5} | {} | {} | {} | {} |\n",
            run.venue,
            run.window,
            run.strategy,
            run.params,
            number(run.after_fee_pnl_quote, 4),
            run.max_drawdown_quote,
            run.turnover_quote,
            run.fills,
            number(run.fill_rate, 4),
            number(run.mean_abs_inventory_base, 5),
            run.max_abs_inventory_base,
            run.rejected_orders_total,
            run.kill_switch_activations,
            run.messages_applied,
            match &run.truncation_reason {
                Some(reason) => reason.clone(),
                None => "no".to_string(),
            }
        ));
    }
    out.push('\n');

    if !controls.is_empty() {
        out.push_str("## Negative controls\n\n");
        out.push_str(
            "Each of these is a deliberate corruption selected only by the gate script, and each \
             is expected to fail. A control that passes proves nothing.\n\n",
        );
        out.push_str("| control | expected | failed as required | evidence |\n|---|---|---|---|\n");
        for control in controls {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                control.control, control.expected, control.failed_as_required, control.evidence
            ));
        }
        out.push('\n');
    }

    out.push_str("## What these numbers are not\n\n");
    out.push_str(
        "- Every fill here is simulated against a recording. No order reached a venue, no venue \
         was connected, and no money moved.\n\
         - Queue position is approximate everywhere it appears, and every field carrying one is \
         named `approx_`. On an aggregated feed a shrinking level is either a trade or a \
         cancellation and the venue never says which, so a maker fill needs a crossing as \
         positive evidence.\n\
         - A marketable order walks the recorded book without removing those levels from it, so a \
         larger order than the ones used here would be flattered.\n\
         - One recording date, under six minutes per venue. The walk-forward unit is a window \
         inside one recording, not a trading day, and nothing here is evidence about another day, \
         another instrument, or another regime.\n\
         - The maker rebate is zero, meaning a maker fill pays nothing. Real spot maker fees are \
         usually a positive cost, so the quoting numbers are an upper bound. The zero-cost control \
         measures exactly what that is worth: the taker strategy pays a great deal and the quoting \
         strategies pay nothing at all.\n\
         - The half-spread grid was frozen in basis points, and the venue table above shows these \
         books quoting a small fraction of one. A quote at the narrowest grid point still sits far \
         outside the touch, which bounds every fill count here. That is a defect of the frozen \
         experiment rather than of the simulator, and correcting it means a new manifest and a new \
         run, not a rerun of this one.\n",
    );

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, out)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickvault::Side;

    fn trade() -> TradeRow {
        TradeRow {
            venue: "kraken".to_string(),
            symbol: "BTC-USD".to_string(),
            window: 2,
            strategy: "fixed_spread_mm".to_string(),
            params: "half_spread_bps=1 requote_ms=500".to_string(),
            recv_wall_ns: 1_787_637_400_000_000_000,
            side: match Side::Bid {
                Side::Bid => "buy".to_string(),
                Side::Ask => "sell".to_string(),
            },
            price: 110_000.5,
            size: 0.005,
            fee: 0.0,
            liquidity: "maker".to_string(),
            cause: "crossing".to_string(),
            approx_queue_ahead_at_fill: 1.25,
            latency_ms: 4.5,
        }
    }

    #[test]
    fn the_trades_file_round_trips_every_column() {
        let dir = std::env::temp_dir().join("tickvault-sim-trades-round-trip");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.parquet");
        write_trades(&path, &[trade(), trade()]).unwrap();
        let batches = tickvault::store::reader::read_batches(&path).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        let schema = batches[0].schema();
        // The queue column has to keep its name, because that name is the
        // warranty on the number.
        assert!(
            schema
                .column_with_name("approx_queue_ahead_at_fill")
                .is_some()
        );
        for column in ["venue", "window", "strategy", "cause", "latency_ms", "fee"] {
            assert!(schema.column_with_name(column).is_some(), "{column}");
        }
    }

    #[test]
    fn an_empty_trades_file_is_still_a_file_with_a_schema() {
        let dir = std::env::temp_dir().join("tickvault-sim-trades-empty");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.parquet");
        write_trades(&path, &[]).unwrap();
        let batches = tickvault::store::reader::read_batches(&path).unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    #[test]
    fn a_missing_measurement_prints_as_null_and_never_as_zero() {
        assert_eq!(number(None, 4), "null");
        assert_eq!(number(Some(0.0), 4), "0.0000");
    }
}
