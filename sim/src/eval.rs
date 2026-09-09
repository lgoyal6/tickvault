//! Walk-forward evaluation: fitting on earlier windows, running on later ones.
//!
//! The order of operations inside a window is the part worth reading, because
//! every leak a backtest can have is an ordering mistake.
//!
//! 1. The message's own market data latency is drawn and it is queued to become
//!    visible later. Anything whose visible time has arrived is applied to the
//!    **delayed** book, which is the only book a strategy ever sees.
//! 2. The message is applied to the **true** book and judged by the venue's
//!    sequence scheme. A violation stops the window there.
//! 3. Our resting orders are worked against what the message did, which is the
//!    only place a fill can happen.
//! 4. Orders whose entry latency has elapsed become live. After the fills, so
//!    an order cannot fill from the message that arrived at the instant it
//!    reached the venue.
//! 5. Risk samples the state and fires the switch if a limit broke.
//! 6. Only now does the strategy decide, from the delayed book, and every
//!    decision is checked for leakage before it is acted on.
//!
//! Fitting never touches a held-out window and never crosses venues. For a
//! held-out window `k`, every grid point is run on that venue's windows `0` to
//! `k - 1` and the one with the highest summed after-fee PnL is the one that
//! runs on `k`. Ties break to lower turnover and then to the manifest's own
//! enumeration order, so the choice is the same on every run.

use std::collections::{BTreeMap, VecDeque};

use serde::Serialize;
use tickvault::Side;

use crate::costs::{Costs, Latency, derive_seed, quantile};
use crate::feed::{ReplayState, SeqTamper, VenueFeed};
use crate::manifest::{Loaded, Window};
use crate::market::{Market, levels_before, mid_of, round_to_tick};
use crate::risk::{Rejection, Risk};
use crate::strategy::{Action, Features, Params, PortfolioView, Strategy, grid};
use crate::{SimResult, bail};

/// Which deliberate corruption, if any, this run carries.
///
/// Never a default. Each of these is selected by the gate script and each run
/// is expected to fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    None,
    /// Put a value from the future into a feature. The leakage detector must
    /// catch it.
    LeakFuture,
    /// Charge nothing. The result must be refused as non-comparable.
    NoCosts,
    /// Corrupt the sequence identifiers. The replay invariants must fail.
    ShuffleSeq,
}

impl Control {
    pub fn parse(s: &str) -> SimResult<Control> {
        Ok(match s {
            "none" => Control::None,
            "leak-future" => Control::LeakFuture,
            "no-costs" => Control::NoCosts,
            "shuffle-seq" => Control::ShuffleSeq,
            other => bail!("unknown control {other}"),
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Control::None => "none",
            Control::LeakFuture => "leak-future",
            Control::NoCosts => "no-costs",
            Control::ShuffleSeq => "shuffle-seq",
        }
    }
}

/// One simulated execution, flattened for the trades file.
#[derive(Debug, Clone, Serialize)]
pub struct TradeRow {
    pub venue: String,
    pub symbol: String,
    pub window: u32,
    pub strategy: String,
    pub params: String,
    pub recv_wall_ns: i64,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub liquidity: String,
    pub cause: String,
    /// Approximate. See `market` for exactly how approximate.
    pub approx_queue_ahead_at_fill: f64,
    pub latency_ms: f64,
}

/// What one window of one strategy did.
#[derive(Debug, Clone, Serialize)]
pub struct WindowRun {
    pub venue: String,
    pub symbol: String,
    pub window: usize,
    pub verifiable: bool,
    pub strategy: String,
    pub params: String,
    /// Cash plus inventory marked at the closing mid, after fees. `None` when
    /// inventory was left against a book with no mid to mark it at.
    pub after_fee_pnl_quote: Option<f64>,
    /// The same, with the closing inventory taken out at the touch and charged
    /// the taker fee. Always at or below the marked figure.
    pub after_fee_pnl_liquidated_quote: Option<f64>,
    pub max_drawdown_quote: f64,
    pub turnover_quote: f64,
    /// Orders that got at least one fill over orders submitted. `None` when
    /// nothing was submitted: no orders is not a fill rate of zero.
    pub fill_rate: Option<f64>,
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub fills: usize,
    pub maker_fills_from_crossing: u64,
    pub maker_fills_from_trade: u64,
    pub taker_fills: u64,
    pub mean_abs_inventory_base: Option<f64>,
    pub max_abs_inventory_base: f64,
    pub rejected_orders: BTreeMap<String, u64>,
    pub rejected_orders_total: u64,
    pub kill_switch_activations: u64,
    pub risk_controls_held: bool,
    pub latency_median_ms: Option<f64>,
    pub latency_p99_ms: Option<f64>,
    pub decisions: u64,
    pub leakage_violations: u64,
    pub messages_in_window: usize,
    pub messages_applied: usize,
    pub truncated: bool,
    pub truncation_reason: Option<String>,
    pub seq_in_order: u64,
    pub seq_unverifiable: u64,
    pub counter_forward_skips: u64,
    #[serde(skip)]
    pub trades: Vec<TradeRow>,
}

impl WindowRun {
    fn pnl_or_worst(&self) -> f64 {
        self.after_fee_pnl_quote.unwrap_or(f64::NEG_INFINITY)
    }
}

/// Which parameters were chosen for a held-out window, and on what.
#[derive(Debug, Clone, Serialize)]
pub struct FitRecord {
    pub venue: String,
    pub strategy: String,
    pub held_out_window: usize,
    pub fitted_on_windows: Vec<usize>,
    pub grid_points: usize,
    pub chosen: String,
    pub training_after_fee_pnl_quote: f64,
}

/// A strategy's held-out totals over the venues in gate scope.
#[derive(Debug, Clone, Serialize)]
pub struct StrategyAggregate {
    pub strategy: String,
    pub held_out_windows: usize,
    pub after_fee_pnl_quote: f64,
    pub after_fee_pnl_liquidated_quote: f64,
    pub max_drawdown_quote: f64,
    pub turnover_quote: f64,
    pub fills: usize,
    pub orders_submitted: u64,
    pub fill_rate: Option<f64>,
    pub max_abs_inventory_base: f64,
    pub mean_abs_inventory_base: Option<f64>,
    pub rejected_orders: BTreeMap<String, u64>,
    pub kill_switch_activations: u64,
    pub latency_median_ms: Option<f64>,
    pub latency_p99_ms: Option<f64>,
    pub windows_with_positive_pnl: usize,
    /// Held-out windows whose PnL sign matches the aggregate's.
    pub windows_agreeing_with_aggregate: usize,
}

/// A block bootstrap interval.
#[derive(Debug, Clone, Serialize)]
pub struct BootstrapInterval {
    pub statistic: String,
    pub unit: String,
    pub units: usize,
    pub block_length: usize,
    pub resamples: usize,
    pub point_estimate: f64,
    pub lower_95: Option<f64>,
    pub upper_95: Option<f64>,
}

/// One promotion gate clause and whether it held.
#[derive(Debug, Clone, Serialize)]
pub struct GateClause {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Gate {
    pub clauses: Vec<GateClause>,
    pub strategy_quality: String,
}

/// Per-venue facts a reader needs to judge the numbers.
#[derive(Debug, Clone, Serialize)]
pub struct VenueSummary {
    pub venue: String,
    pub symbol: String,
    pub sequence_scheme: String,
    pub verifiable: bool,
    pub in_gate_scope: bool,
    pub messages: usize,
    pub observed_tick: String,
    /// Median spread of the recording in basis points of the mid. Measured, and
    /// the number that says how far outside the touch the frozen half-spread
    /// grid actually put a quote.
    pub median_spread_bps: Option<f64>,
    pub warmup_counter_forward_skips: u64,
    pub warmup_unverifiable: u64,
}

/// Everything one evaluation produced.
#[derive(Debug, Clone, Serialize)]
pub struct Evaluation {
    pub manifest_sha256: String,
    pub data_boundary: String,
    pub control: String,
    pub comparable: bool,
    pub comparable_note: String,
    pub costs_taker_fee_bps: f64,
    pub costs_maker_rebate_bps: f64,
    pub costs_extra_slippage_bps: f64,
    pub venues: Vec<VenueSummary>,
    pub fits: Vec<FitRecord>,
    pub runs: Vec<WindowRun>,
    pub aggregates: Vec<StrategyAggregate>,
    pub bootstrap: BootstrapInterval,
    pub direction_agreement: usize,
    pub direction_agreement_required: usize,
    pub leakage_violations: u64,
    pub invariant_violations: u64,
    pub truncated_windows: usize,
    pub gate: Gate,
    #[serde(skip)]
    pub trades: Vec<TradeRow>,
}

/// Everything one window of one strategy needs.
struct RunContext<'a> {
    feed: &'a VenueFeed,
    loaded: &'a Loaded,
    costs: Costs,
    control: Control,
}

fn run_window(
    ctx: &RunContext<'_>,
    window: &Window,
    start: ReplayState,
    params: Params,
    param_index: usize,
    keep_trades: bool,
) -> WindowRun {
    let manifest = &ctx.loaded.manifest;
    let venue = ctx.feed.venue.as_str().to_string();
    let symbol = ctx.feed.symbol.as_str().to_string();
    let (from, to) = ctx.feed.range_of(window);
    let messages = &ctx.feed.messages[from..to];
    let tamper = match ctx.control {
        Control::ShuffleSeq => SeqTamper::for_window(messages.len()),
        _ => SeqTamper::default(),
    };

    let seed = derive_seed(
        manifest.latency_model.base_seed,
        &venue,
        window.index,
        params.strategy_name(),
        param_index,
    );
    let mut latency = Latency::new(&manifest.latency_model, seed);
    let mut state = start;
    let seq_before = state.enforcer.counts;
    let mut visible = state.replayer.clone();
    let mut market = Market::new(ctx.costs, ctx.feed.observed_tick);
    let mut risk = Risk::new(manifest.risk_limits);
    let mut strategy = Strategy::new(params);

    let mut pending_md: VecDeque<(i64, usize)> = VecDeque::new();
    let mut max_source_recv_wall = window.from_recv_wall_ns;
    let mut max_source_visible_at = window.from_recv_wall_ns;
    let mut equity_peak: Option<f64> = None;
    let mut max_drawdown = 0.0f64;
    let mut decisions = 0u64;
    let mut leakage_violations = 0u64;
    let mut applied = 0usize;
    let mut truncation: Option<String> = None;

    for (local, message) in messages.iter().enumerate() {
        let now = message.recv_wall;

        // 1. This message becomes visible to the strategy later, never now.
        pending_md.push_back((now + latency.market_data_nanos(), local));
        while let Some((visible_at, index)) = pending_md.front().copied() {
            if visible_at > now {
                break;
            }
            pending_md.pop_front();
            visible.apply_message(&messages[index].rows);
            max_source_recv_wall = max_source_recv_wall.max(messages[index].recv_wall);
            max_source_visible_at = max_source_visible_at.max(visible_at);
        }

        // 2. Apply to the true book and judge it.
        let before = levels_before(state.book(), &message.rows);
        let ident = tamper.ident_at(local, messages);
        if tamper.duplicate_at == Some(local)
            && let Err(violation) = state.apply(message, ident)
        {
            truncation = Some(violation.to_string());
            break;
        }
        if let Err(violation) = state.apply(message, ident) {
            truncation = Some(violation.to_string());
            break;
        }
        applied += 1;

        // 3. Work our orders against what the message did.
        if message.event == tickvault::store::schema::EventKind::Snapshot {
            let book = state.book().clone();
            market.reseed_queues(&book);
        }
        let book = state.book().clone();
        market.on_message(&before, message.traded_qty(), &book, now);

        // 4. Orders whose latency has elapsed go live, after the fills.
        market.activate(&book, now);

        // 5. Risk.
        let true_mid = mid_of(&book);
        let equity = market.equity(true_mid);
        if let Some(equity) = equity {
            let peak = equity_peak.get_or_insert(equity);
            *peak = peak.max(equity);
            max_drawdown = max_drawdown.max(*peak - equity);
        }
        let breaches = risk.observe(&market, true_mid, equity);
        if !breaches.is_empty() {
            let entry = latency.order_entry_nanos();
            market.cancel_all(now, entry);
        }

        // 6. The strategy sees the delayed book and nothing else.
        let visible_book = visible.book_ref();
        let mut features = Features::from_book(
            visible_book,
            now,
            max_source_recv_wall,
            max_source_visible_at,
        );
        if ctx.control == Control::LeakFuture
            && let Some(next) = messages.get(local + 1)
            && let Some(row) = next.rows.first()
        {
            // A value from a message that has not arrived, let alone become
            // visible. This is the whole control.
            features.mid = Some(row.price.to_f64_lossy());
            features.max_source_recv_wall = next.recv_wall;
            features.max_source_visible_at = next.recv_wall + 1;
        }
        decisions += 1;
        if features.leaks() {
            leakage_violations += 1;
        }

        let view = PortfolioView {
            inventory: market.inventory,
            resting: market
                .resting()
                .iter()
                .map(|o| (o.id, o.side, o.price.to_f64_lossy(), o.remaining))
                .collect(),
        };
        let actions = strategy.decide(&features, &view, manifest.risk_limits.inventory_limit_base);
        for action in actions {
            match action {
                Action::CancelAll => {
                    let entry = latency.order_entry_nanos();
                    market.cancel_all(now, entry);
                }
                Action::PlaceMaker { side, price, size } => {
                    let intent = crate::risk::Intent {
                        side,
                        size,
                        price: Some(price),
                    };
                    if risk.admit(intent, &market, features.mid).is_err() {
                        continue;
                    }
                    if risk
                        .admit_maker_price(
                            side,
                            price,
                            features.best_bid.map(|(p, _)| p),
                            features.best_ask.map(|(p, _)| p),
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let entry = latency.order_entry_nanos();
                    let entry_ms = latency.last_entry_ms();
                    let on_grid = round_to_tick(price, market.tick(), side);
                    market.submit_maker(side, on_grid, size, now, entry, entry_ms);
                }
                Action::PlaceTaker { side, size } => {
                    let intent = crate::risk::Intent {
                        side,
                        size,
                        price: None,
                    };
                    if risk.admit(intent, &market, features.mid).is_err() {
                        continue;
                    }
                    let entry = latency.order_entry_nanos();
                    let entry_ms = latency.last_entry_ms();
                    market.submit_taker(side, size, now, entry, entry_ms);
                }
            }
        }
    }

    let closing_mid = market.last_mid();
    let after_fee_pnl_quote = market.equity(closing_mid);
    let after_fee_pnl_liquidated_quote = liquidated(&market, ctx.costs, closing_mid);
    let seq_after = state.enforcer.counts;
    let counts = market.counts;
    let all_samples = latency.all_samples();
    let params_label = params.label();

    let trades = if keep_trades {
        market
            .fills
            .iter()
            .map(|fill| TradeRow {
                venue: venue.clone(),
                symbol: symbol.clone(),
                window: window.index as u32,
                strategy: params.strategy_name().to_string(),
                params: params_label.clone(),
                recv_wall_ns: fill.at_recv_wall,
                side: match fill.side {
                    Side::Bid => "buy".to_string(),
                    Side::Ask => "sell".to_string(),
                },
                price: fill.price,
                size: fill.size,
                fee: fill.fee,
                liquidity: fill.liquidity.as_str().to_string(),
                cause: fill.cause.as_str().to_string(),
                approx_queue_ahead_at_fill: fill.approx_queue_ahead_at_fill,
                latency_ms: fill.latency_ms,
            })
            .collect()
    } else {
        Vec::new()
    };

    let mut rejected: BTreeMap<String, u64> = BTreeMap::new();
    for reason in Rejection::ALL {
        let n = risk.rejections().get(reason).copied().unwrap_or(0);
        if n > 0 {
            rejected.insert(reason.as_str().to_string(), n);
        }
    }

    WindowRun {
        venue,
        symbol,
        window: window.index,
        verifiable: ctx.feed.verifiable,
        strategy: params.strategy_name().to_string(),
        params: params_label,
        after_fee_pnl_quote,
        after_fee_pnl_liquidated_quote,
        max_drawdown_quote: max_drawdown,
        turnover_quote: market.turnover,
        fill_rate: (counts.orders_submitted > 0)
            .then(|| counts.orders_filled as f64 / counts.orders_submitted as f64),
        orders_submitted: counts.orders_submitted,
        orders_filled: counts.orders_filled,
        fills: market.fills.len(),
        maker_fills_from_crossing: counts.maker_fills_from_crossing,
        maker_fills_from_trade: counts.maker_fills_from_trade,
        taker_fills: counts.taker_fills,
        mean_abs_inventory_base: risk.mean_abs_inventory(),
        max_abs_inventory_base: risk.max_abs_inventory(),
        rejected_orders_total: risk.total_rejections(),
        rejected_orders: rejected,
        kill_switch_activations: risk.activations(),
        risk_controls_held: risk.controls_held(),
        latency_median_ms: quantile(&all_samples, 0.5),
        latency_p99_ms: quantile(&all_samples, 0.99),
        decisions,
        leakage_violations,
        messages_in_window: messages.len(),
        messages_applied: applied,
        truncated: truncation.is_some(),
        truncation_reason: truncation,
        seq_in_order: seq_after.in_order - seq_before.in_order,
        seq_unverifiable: seq_after.unverifiable - seq_before.unverifiable,
        counter_forward_skips: seq_after.counter_forward_skips - seq_before.counter_forward_skips,
        trades,
    }
}

/// PnL with the closing position taken out at the touch, paying the taker fee.
///
/// Always at or below the marked figure, because leaving inventory marked at
/// the mid assumes an exit that costs nothing.
fn liquidated(market: &Market, costs: Costs, mid: Option<f64>) -> Option<f64> {
    if market.inventory.abs() <= 1e-12 {
        return Some(market.cash);
    }
    let mid = mid?;
    let notional = market.inventory.abs() * mid;
    Some(market.cash + market.inventory * mid - costs.taker_fee(notional))
}

/// Fit a strategy on the windows strictly before `held_out`.
fn fit(
    ctx: &RunContext<'_>,
    windows: &[Window],
    states: &[ReplayState],
    points: &[Params],
    held_out: usize,
) -> (usize, f64) {
    let mut best: Option<(usize, f64, f64)> = None;
    for (index, params) in points.iter().enumerate() {
        let mut pnl = 0.0f64;
        let mut turnover = 0.0f64;
        for train in 0..held_out {
            let run = run_window(
                ctx,
                &windows[train],
                states[train].clone(),
                *params,
                index,
                false,
            );
            pnl += run.pnl_or_worst();
            turnover += run.turnover_quote;
        }
        let better = match best {
            None => true,
            Some((_, best_pnl, best_turnover)) => {
                pnl > best_pnl || (pnl == best_pnl && turnover < best_turnover)
            }
        };
        if better {
            best = Some((index, pnl, turnover));
        }
    }
    let (index, pnl, _) = best.expect("a grid always has at least one point");
    (index, pnl)
}

/// Run the whole experiment.
pub fn evaluate(loaded: &Loaded, control: Control) -> SimResult<Evaluation> {
    let manifest = &loaded.manifest;
    let costs = match control {
        Control::NoCosts => Costs::zeroed(),
        _ => Costs::from_spec(&manifest.costs),
    };

    let mut order: Vec<&str> = vec![manifest.candidate.as_str()];
    order.extend(manifest.baselines.iter().map(|s| s.as_str()));
    order.sort_unstable();
    order.dedup();

    let mut venues = Vec::new();
    let mut runs: Vec<WindowRun> = Vec::new();
    let mut fits: Vec<FitRecord> = Vec::new();
    let mut trades: Vec<TradeRow> = Vec::new();

    for input in &manifest.inputs {
        let feed = VenueFeed::load(&loaded.repo_root, input)?;
        let windows = loaded.windows_for(&input.venue)?.to_vec();
        let states = feed.window_states(&windows, manifest.kraken_checksum_precision)?;
        venues.push(VenueSummary {
            venue: input.venue.clone(),
            symbol: input.symbol.clone(),
            sequence_scheme: feed.scheme.as_str().to_string(),
            verifiable: feed.verifiable,
            in_gate_scope: feed.verifiable,
            messages: feed.messages.len(),
            observed_tick: feed.observed_tick.to_decimal_string(9),
            median_spread_bps: feed.median_spread_bps(),
            warmup_counter_forward_skips: states[0].enforcer.counts.counter_forward_skips,
            warmup_unverifiable: states[0].enforcer.counts.unverifiable,
        });

        let ctx = RunContext {
            feed: &feed,
            loaded,
            costs,
            control,
        };

        for name in &order {
            let spec = loaded.spec(name)?;
            let points = grid(name, spec)?;
            for held_out in manifest.windows.held_out_indices.iter().copied() {
                if held_out >= windows.len() {
                    bail!(
                        "held-out window {held_out} does not exist for {}",
                        input.venue
                    );
                }
                let (chosen, training_pnl) = if points.len() > 1 {
                    fit(&ctx, &windows, &states, &points, held_out)
                } else {
                    (0, 0.0)
                };
                fits.push(FitRecord {
                    venue: input.venue.clone(),
                    strategy: (*name).to_string(),
                    held_out_window: held_out,
                    fitted_on_windows: (0..held_out).collect(),
                    grid_points: points.len(),
                    chosen: points[chosen].label(),
                    training_after_fee_pnl_quote: training_pnl,
                });
                let mut run = run_window(
                    &ctx,
                    &windows[held_out],
                    states[held_out].clone(),
                    points[chosen],
                    chosen,
                    true,
                );
                trades.append(&mut run.trades);
                runs.push(run);
            }
        }
    }

    let aggregates = aggregate(&runs, &order);
    let leakage_violations: u64 = runs.iter().map(|r| r.leakage_violations).sum();
    let truncated_windows = runs.iter().filter(|r| r.truncated).count();
    // A truncation is one detected invariant violation, whichever strategy was
    // running when it was hit.
    let invariant_violations = runs.iter().filter(|r| r.truncated && r.verifiable).count() as u64;

    let candidate = manifest.candidate.clone();
    let series = candidate_series(&runs, &candidate);
    let bootstrap = block_bootstrap(
        &series,
        manifest.bootstrap.block_length,
        manifest.bootstrap.resamples,
        manifest.bootstrap.seed,
    );
    let direction_agreement =
        direction_agreement(&runs, &candidate, &manifest.windows.held_out_indices);

    let comparable = control != Control::NoCosts;
    let comparable_note = if comparable {
        "run under the frozen cost block".to_string()
    } else {
        "fees, rebates and slippage were all set to zero, so these numbers are not comparable to \
         a run that paid"
            .to_string()
    };

    let gate = evaluate_gate(
        &aggregates,
        &runs,
        &bootstrap,
        direction_agreement,
        leakage_violations,
        invariant_violations,
        comparable,
        &candidate,
        &manifest.baselines,
    );

    Ok(Evaluation {
        manifest_sha256: loaded.sha256.clone(),
        data_boundary: manifest.data_boundary.clone(),
        control: control.as_str().to_string(),
        comparable,
        comparable_note,
        costs_taker_fee_bps: costs.taker_fee_bps,
        costs_maker_rebate_bps: costs.maker_rebate_bps,
        costs_extra_slippage_bps: costs.extra_slippage_bps,
        venues,
        fits,
        runs,
        aggregates,
        bootstrap,
        direction_agreement,
        direction_agreement_required: 3,
        leakage_violations,
        invariant_violations,
        truncated_windows,
        gate,
        trades,
    })
}

/// Held-out runs of one strategy on the venues in gate scope, in chronological
/// order.
fn candidate_series(runs: &[WindowRun], strategy: &str) -> Vec<f64> {
    let mut selected: Vec<&WindowRun> = runs
        .iter()
        .filter(|r| r.strategy == strategy && r.verifiable)
        .collect();
    selected.sort_by(|a, b| (a.window, &a.venue).cmp(&(b.window, &b.venue)));
    selected
        .iter()
        .map(|r| r.after_fee_pnl_quote.unwrap_or(0.0))
        .collect()
}

fn aggregate(runs: &[WindowRun], order: &[&str]) -> Vec<StrategyAggregate> {
    let mut out = Vec::new();
    for name in order {
        let selected: Vec<&WindowRun> = runs
            .iter()
            .filter(|r| r.strategy == *name && r.verifiable)
            .collect();
        if selected.is_empty() {
            continue;
        }
        let pnl: f64 = selected.iter().filter_map(|r| r.after_fee_pnl_quote).sum();
        let liquidated: f64 = selected
            .iter()
            .filter_map(|r| r.after_fee_pnl_liquidated_quote)
            .sum();
        let submitted: u64 = selected.iter().map(|r| r.orders_submitted).sum();
        let filled: u64 = selected.iter().map(|r| r.orders_filled).sum();
        let mut rejected: BTreeMap<String, u64> = BTreeMap::new();
        for run in &selected {
            for (reason, n) in &run.rejected_orders {
                *rejected.entry(reason.clone()).or_insert(0) += n;
            }
        }
        let inventories: Vec<f64> = selected
            .iter()
            .filter_map(|r| r.mean_abs_inventory_base)
            .collect();
        let medians: Vec<f64> = selected
            .iter()
            .filter_map(|r| r.latency_median_ms)
            .collect();
        let p99s: Vec<f64> = selected.iter().filter_map(|r| r.latency_p99_ms).collect();
        let positive = selected
            .iter()
            .filter(|r| r.after_fee_pnl_quote.unwrap_or(0.0) > 0.0)
            .count();
        let agreeing = selected
            .iter()
            .filter(|r| {
                let v = r.after_fee_pnl_quote.unwrap_or(0.0);
                (v > 0.0 && pnl > 0.0) || (v < 0.0 && pnl < 0.0)
            })
            .count();
        out.push(StrategyAggregate {
            strategy: (*name).to_string(),
            held_out_windows: selected.len(),
            after_fee_pnl_quote: pnl,
            after_fee_pnl_liquidated_quote: liquidated,
            max_drawdown_quote: selected
                .iter()
                .map(|r| r.max_drawdown_quote)
                .fold(0.0, f64::max),
            turnover_quote: selected.iter().map(|r| r.turnover_quote).sum(),
            fills: selected.iter().map(|r| r.fills).sum(),
            orders_submitted: submitted,
            fill_rate: (submitted > 0).then(|| filled as f64 / submitted as f64),
            max_abs_inventory_base: selected
                .iter()
                .map(|r| r.max_abs_inventory_base)
                .fold(0.0, f64::max),
            mean_abs_inventory_base: (!inventories.is_empty())
                .then(|| inventories.iter().sum::<f64>() / inventories.len() as f64),
            rejected_orders: rejected,
            kill_switch_activations: selected.iter().map(|r| r.kill_switch_activations).sum(),
            latency_median_ms: quantile(&medians, 0.5),
            latency_p99_ms: quantile(&p99s, 0.99),
            windows_with_positive_pnl: positive,
            windows_agreeing_with_aggregate: agreeing,
        });
    }
    out
}

/// How many chronological window positions agree with the overall sign.
///
/// The unit is the window position, not the individual run, because "direction
/// agrees across chronological windows" is a statement about time rather than
/// about how many venues happened to be recorded.
fn direction_agreement(runs: &[WindowRun], strategy: &str, held_out: &[usize]) -> usize {
    let overall: f64 = runs
        .iter()
        .filter(|r| r.strategy == strategy && r.verifiable)
        .filter_map(|r| r.after_fee_pnl_quote)
        .sum();
    if overall == 0.0 {
        return 0;
    }
    held_out
        .iter()
        .filter(|index| {
            let at_position: f64 = runs
                .iter()
                .filter(|r| r.strategy == strategy && r.verifiable && r.window == **index)
                .filter_map(|r| r.after_fee_pnl_quote)
                .sum();
            at_position.signum() == overall.signum() && at_position != 0.0
        })
        .count()
}

/// A moving-block bootstrap of the total over a chronological series.
///
/// Blocks rather than individual draws because consecutive windows of one
/// recording are not independent, and resampling them one at a time would
/// produce an interval far narrower than the data supports.
pub fn block_bootstrap(
    series: &[f64],
    block_length: usize,
    resamples: usize,
    seed: u64,
) -> BootstrapInterval {
    let point: f64 = series.iter().sum();
    let mut interval = BootstrapInterval {
        statistic: "total held-out after-fee PnL in quote units".to_string(),
        unit: "one held-out (venue, window) pair; a single recording date means the unit is a \
               window rather than a trading day"
            .to_string(),
        units: series.len(),
        block_length,
        resamples,
        point_estimate: point,
        lower_95: None,
        upper_95: None,
    };
    let n = series.len();
    if n == 0 || block_length == 0 || block_length > n || resamples == 0 {
        return interval;
    }
    let starts = n - block_length + 1;
    let blocks_needed = n.div_ceil(block_length);
    let mut rng = crate::costs::SplitMix64::new(seed);
    let mut totals = Vec::with_capacity(resamples);
    for _ in 0..resamples {
        let mut total = 0.0;
        let mut taken = 0usize;
        for _ in 0..blocks_needed {
            let start = rng.below(starts);
            for offset in 0..block_length {
                if taken == n {
                    break;
                }
                total += series[start + offset];
                taken += 1;
            }
        }
        totals.push(total);
    }
    interval.lower_95 = quantile(&totals, 0.025);
    interval.upper_95 = quantile(&totals, 0.975);
    interval
}

#[allow(clippy::too_many_arguments)]
fn evaluate_gate(
    aggregates: &[StrategyAggregate],
    runs: &[WindowRun],
    bootstrap: &BootstrapInterval,
    direction_agreement: usize,
    leakage_violations: u64,
    invariant_violations: u64,
    comparable: bool,
    candidate: &str,
    baselines: &[String],
) -> Gate {
    let find = |name: &str| aggregates.iter().find(|a| a.strategy == name);
    let candidate_pnl = find(candidate).map(|a| a.after_fee_pnl_quote);
    let mut clauses = Vec::new();

    clauses.push(GateClause {
        id: "no_invariant_violations".to_string(),
        passed: invariant_violations == 0,
        detail: format!(
            "{invariant_violations} held-out window(s) in scope stopped on a replay invariant"
        ),
    });
    clauses.push(GateClause {
        id: "no_leakage".to_string(),
        passed: leakage_violations == 0,
        detail: format!("{leakage_violations} decision(s) used a feature that was not yet visible"),
    });
    let controls_held = runs.iter().all(|r| r.risk_controls_held);
    clauses.push(GateClause {
        id: "risk_controls_pass".to_string(),
        passed: controls_held,
        detail: if controls_held {
            "every limit breach observed was answered by a kill-switch activation".to_string()
        } else {
            "a run breached a limit without the kill switch firing".to_string()
        },
    });
    for baseline in baselines {
        let baseline_pnl = find(baseline).map(|a| a.after_fee_pnl_quote);
        let passed = match (candidate_pnl, baseline_pnl) {
            (Some(c), Some(b)) => c > b,
            _ => false,
        };
        clauses.push(GateClause {
            id: format!("beats_{baseline}"),
            passed,
            detail: match (candidate_pnl, baseline_pnl) {
                (Some(c), Some(b)) => format!("candidate {c:.4} against {baseline} {b:.4}"),
                _ => format!("{baseline} produced no comparable number"),
            },
        });
    }
    let lower = bootstrap.lower_95;
    clauses.push(GateClause {
        id: "bootstrap_lower_bound_above_zero".to_string(),
        passed: lower.is_some_and(|l| l > 0.0),
        detail: match (bootstrap.lower_95, bootstrap.upper_95) {
            (Some(l), Some(u)) => format!("95 percent interval [{l:.4}, {u:.4}]"),
            _ => "no interval could be formed".to_string(),
        },
    });
    clauses.push(GateClause {
        id: "direction_agreement".to_string(),
        passed: direction_agreement >= 3,
        detail: format!("{direction_agreement} of 4 chronological window positions agree in sign"),
    });
    if !comparable {
        clauses.push(GateClause {
            id: "comparable_costs".to_string(),
            passed: false,
            detail:
                "this run charged nothing, so its numbers cannot be compared to a run that paid"
                    .to_string(),
        });
    }

    let passed = clauses.iter().all(|c| c.passed);
    Gate {
        strategy_quality: if passed {
            "Promoted".to_string()
        } else {
            "Rejected".to_string()
        },
        clauses,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_control_is_never_selected_by_accident() {
        assert_eq!(Control::parse("none").unwrap(), Control::None);
        assert_eq!(Control::parse("leak-future").unwrap(), Control::LeakFuture);
        assert_eq!(Control::parse("no-costs").unwrap(), Control::NoCosts);
        assert_eq!(Control::parse("shuffle-seq").unwrap(), Control::ShuffleSeq);
        assert!(Control::parse("").is_err());
        assert!(Control::parse("leak_future").is_err());
    }

    #[test]
    fn a_bootstrap_over_a_constant_series_is_a_point() {
        let series = vec![2.0; 8];
        let interval = block_bootstrap(&series, 2, 500, 1);
        assert_eq!(interval.point_estimate, 16.0);
        assert_eq!(interval.lower_95, Some(16.0));
        assert_eq!(interval.upper_95, Some(16.0));
    }

    #[test]
    fn a_bootstrap_over_a_series_that_straddles_zero_does_not_exclude_it() {
        // Windows that won and lost by similar amounts must not produce an
        // interval that claims a direction.
        let series = vec![5.0, -3.0, -4.0, 6.0, -1.0, 2.0, -6.0, 1.0];
        let interval = block_bootstrap(&series, 2, 2_000, 7);
        let lower = interval.lower_95.unwrap();
        let upper = interval.upper_95.unwrap();
        assert!(lower < 0.0 && upper > 0.0, "[{lower}, {upper}]");
    }

    #[test]
    fn a_bootstrap_over_a_series_that_never_loses_excludes_zero() {
        // This is the gate clause itself: a lower bound above zero has to be
        // reachable, or the gate is unpassable by construction rather than by
        // measurement.
        let series = vec![3.0, 4.0, 2.0, 5.0, 3.5, 4.5, 2.5, 3.0];
        let interval = block_bootstrap(&series, 2, 2_000, 11);
        assert!(interval.lower_95.unwrap() > 0.0);
    }

    #[test]
    fn a_bootstrap_is_the_same_every_time_it_is_run() {
        let series = vec![1.0, -0.5, 2.0, 0.25, -1.5, 3.0];
        let a = block_bootstrap(&series, 2, 1_000, 20260826);
        let b = block_bootstrap(&series, 2, 1_000, 20260826);
        assert_eq!(a.lower_95, b.lower_95);
        assert_eq!(a.upper_95, b.upper_95);
    }

    #[test]
    fn a_bootstrap_with_nothing_to_resample_says_nothing() {
        let interval = block_bootstrap(&[], 2, 100, 1);
        assert_eq!(interval.lower_95, None);
        assert_eq!(interval.upper_95, None);
        assert_eq!(interval.units, 0);
    }

    fn run(strategy: &str, window: usize, venue: &str, pnl: f64) -> WindowRun {
        WindowRun {
            venue: venue.to_string(),
            symbol: "BTC-USD".to_string(),
            window,
            verifiable: true,
            strategy: strategy.to_string(),
            params: "none".to_string(),
            after_fee_pnl_quote: Some(pnl),
            after_fee_pnl_liquidated_quote: Some(pnl),
            max_drawdown_quote: 0.0,
            turnover_quote: 0.0,
            fill_rate: None,
            orders_submitted: 0,
            orders_filled: 0,
            fills: 0,
            maker_fills_from_crossing: 0,
            maker_fills_from_trade: 0,
            taker_fills: 0,
            mean_abs_inventory_base: None,
            max_abs_inventory_base: 0.0,
            rejected_orders: BTreeMap::new(),
            rejected_orders_total: 0,
            kill_switch_activations: 0,
            risk_controls_held: true,
            latency_median_ms: None,
            latency_p99_ms: None,
            decisions: 0,
            leakage_violations: 0,
            messages_in_window: 0,
            messages_applied: 0,
            truncated: false,
            truncation_reason: None,
            seq_in_order: 0,
            seq_unverifiable: 0,
            counter_forward_skips: 0,
            trades: Vec::new(),
        }
    }

    #[test]
    fn direction_agreement_counts_window_positions_not_runs() {
        // Three of the four positions are positive, and the fourth is a large
        // negative that the total survives.
        let runs = vec![
            run("c", 1, "a", 3.0),
            run("c", 1, "b", 3.0),
            run("c", 2, "a", 2.0),
            run("c", 3, "a", 2.0),
            run("c", 4, "a", -4.0),
        ];
        assert_eq!(direction_agreement(&runs, "c", &[1, 2, 3, 4]), 3);
    }

    #[test]
    fn an_unverifiable_venue_is_kept_out_of_the_aggregate() {
        let mut unverifiable = run("c", 1, "bitstamp", 1_000.0);
        unverifiable.verifiable = false;
        let runs = vec![run("c", 1, "kraken", 1.0), unverifiable];
        let aggregates = aggregate(&runs, &["c"]);
        assert_eq!(aggregates[0].after_fee_pnl_quote, 1.0);
        assert_eq!(aggregates[0].held_out_windows, 1);
    }

    #[test]
    fn a_losing_candidate_is_rejected_and_says_which_clause_failed() {
        let runs = vec![run("cand", 1, "a", -1.0), run("base", 1, "a", 2.0)];
        let aggregates = aggregate(&runs, &["base", "cand"]);
        let bootstrap = block_bootstrap(&[-1.0, -1.0, -1.0, -1.0], 2, 200, 1);
        let gate = evaluate_gate(
            &aggregates,
            &runs,
            &bootstrap,
            0,
            0,
            0,
            true,
            "cand",
            &["base".to_string()],
        );
        assert_eq!(gate.strategy_quality, "Rejected");
        assert!(
            gate.clauses
                .iter()
                .any(|c| c.id == "beats_base" && !c.passed)
        );
        assert!(
            gate.clauses
                .iter()
                .any(|c| c.id == "bootstrap_lower_bound_above_zero" && !c.passed)
        );
    }

    #[test]
    fn leakage_alone_rejects_however_good_the_pnl_is() {
        let runs = vec![run("cand", 1, "a", 1_000.0), run("base", 1, "a", -5.0)];
        let aggregates = aggregate(&runs, &["base", "cand"]);
        let bootstrap = block_bootstrap(&[250.0; 4], 2, 200, 1);
        let gate = evaluate_gate(
            &aggregates,
            &runs,
            &bootstrap,
            4,
            1,
            0,
            true,
            "cand",
            &["base".to_string()],
        );
        assert_eq!(gate.strategy_quality, "Rejected");
        assert!(
            gate.clauses
                .iter()
                .any(|c| c.id == "no_leakage" && !c.passed)
        );
    }

    #[test]
    fn a_run_that_charged_nothing_can_never_be_promoted() {
        let runs = vec![run("cand", 1, "a", 1_000.0), run("base", 1, "a", -5.0)];
        let aggregates = aggregate(&runs, &["base", "cand"]);
        let bootstrap = block_bootstrap(&[250.0; 4], 2, 200, 1);
        let gate = evaluate_gate(
            &aggregates,
            &runs,
            &bootstrap,
            4,
            0,
            0,
            false,
            "cand",
            &["base".to_string()],
        );
        assert_eq!(gate.strategy_quality, "Rejected");
        assert!(
            gate.clauses
                .iter()
                .any(|c| c.id == "comparable_costs" && !c.passed)
        );
    }
}
