"""Agent 3 — Backtest Execution.

Receives the screened universe each quarter and:
  * builds target weights (equal or score-weighted with a position cap),
  * computes drift-aware turnover vs the live portfolio and applies
    transaction costs in bps on traded notional,
  * simulates the quarter day-by-day for both the strategy and an
    equal-weight buy-the-universe benchmark,
  * tracks the equity curve, running peak and max drawdown.

All state (weights, curves, peak, costs, last quarter processed) lives in
the external store, so this agent can be killed and restarted between or
even during quarters without corrupting the track record. Re-ticked
quarters are dropped idempotently.

If a quarter arrives with zero approved names, the portfolio is held over
unchanged (no forced liquidation into an empty list) and the report is
flagged.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from ..config import PipelineConfig
from ..marketsim import SimulatedWorld
from ..messages import (FatalAgentError, FilteredBatch, HealthKind,
                        RebalanceReport, Shutdown)
from ..store import DataStore
from .base import BaseAgent


class BacktestExecutionAgent(BaseAgent):
    def __init__(self, cfg: PipelineConfig, store: DataStore,
                 inbox: asyncio.Queue, health_q: asyncio.Queue,
                 world: SimulatedWorld, results_out: asyncio.Queue) -> None:
        super().__init__("backtest", cfg, store, inbox, health_q)
        self.world = world
        self.results_out = results_out

    # ------------------------------------------------------------------ main
    async def handle(self, msg: Any) -> None:
        if not isinstance(msg, FilteredBatch):
            self.log.warning("ignoring unexpected message %s", type(msg).__name__)
            return
        q = msg.quarter

        crash_q = self.cfg.inject_crash.get("backtest")
        if crash_q == q and self.store.set_flag(f"crash_injected:backtest:{q}"):
            self.log.error("FAULT INJECTION: backtest crashing in quarter %d", q)
            raise FatalAgentError(f"injected crash in quarter {q}")

        p = self.store.portfolio
        if q <= p.last_quarter_processed:
            self.log.info("q%-2d already rebalanced; dropping duplicate batch", q)
            return
        report = self._rebalance_and_simulate(msg)
        p.last_quarter_processed = q
        self.store.quarter_reports.append(report.__dict__.copy())
        self.store.incr("quarters_completed")
        self.log.info(
            "q%-2d rebalanced into %d names | turnover %5.1f%% | qtr ret "
            "%+6.2f%% | NAV %7.4f | bench %7.4f | maxDD %5.2f%%%s",
            q, report.n_holdings, report.turnover * 100,
            report.quarter_return * 100, report.portfolio_value,
            report.benchmark_value, report.max_drawdown_to_date * 100,
            "  [HELD OVER]" if report.held_over else "")
        self.emit_health(HealthKind.INFO, event="rebalance", quarter=q,
                         nav=report.portfolio_value,
                         turnover=report.turnover)
        await self.results_out.put(report)

    async def on_shutdown_msg(self, msg: Shutdown) -> None:
        self.log.debug("final NAV %.4f over %d quarters",
                       self.store.portfolio.value,
                       self.store.portfolio.last_quarter_processed + 1)

    # ----------------------------------------------------------- core logic
    def _target_weights(self, batch: FilteredBatch) -> Dict[str, float]:
        approved = sorted(batch.approved,
                          key=lambda v: (-v.composite_score, v.ticker))
        if not approved:
            return {}
        if self.cfg.weighting == "equal":
            w = 1.0 / len(approved)
            return {v.ticker: w for v in approved}
        # score-weighted with an iterative cap-and-redistribute
        raw = {v.ticker: max(1e-9, v.composite_score) for v in approved}
        total = sum(raw.values())
        weights = {t: s / total for t, s in raw.items()}
        cap = self.cfg.max_position_weight
        for _ in range(12):
            over = {t: w for t, w in weights.items() if w > cap + 1e-12}
            if not over:
                break
            excess = sum(w - cap for w in over.values())
            for t in over:
                weights[t] = cap
            under = {t: w for t, w in weights.items() if w < cap - 1e-12}
            pool = sum(under.values())
            if pool <= 0:
                break
            for t in under:
                weights[t] += excess * under[t] / pool
        norm = sum(weights.values())
        return {t: w / norm for t, w in weights.items()}

    def _rebalance_and_simulate(self, batch: FilteredBatch) -> RebalanceReport:
        cfg, p, q = self.cfg, self.store.portfolio, batch.quarter
        value_start = p.value
        targets = self._target_weights(batch)
        held_over = False
        note = ""
        if not targets:
            targets = dict(p.weights)        # hold what we own; never force-sell
            held_over = True
            note = "no names passed the screen; holding prior portfolio"
            self.log.warning("q%-2d %s", q, note)
            self.store.incr("held_over_quarters")

        current = dict(p.weights)            # post-drift weights from last qtr
        traded_names = set(current) | set(targets)
        turnover = 0.5 * sum(abs(targets.get(t, 0.0) - current.get(t, 0.0))
                             for t in traded_names)
        cost = turnover * 2.0 * (cfg.transaction_cost_bps / 10_000.0) * p.value
        # (one-way bps applied to both legs of traded notional = 2 * turnover)
        p.value -= cost
        p.total_costs += cost

        holdings = {t: w * p.value for t, w in targets.items()}
        if not p.bench_holdings_value:       # benchmark: EW buy-the-universe
            bw = p.benchmark_value / len(self.world.tickers)
            p.bench_holdings_value = {t: bw for t in self.world.tickers}

        days = cfg.trading_days_per_quarter
        paths = {t: self.world.quarter_daily_returns(t, q)
                 for t in set(holdings) | set(p.bench_holdings_value)}
        for d in range(days):
            for t in holdings:
                holdings[t] *= (1.0 + paths[t][d])
            for t in p.bench_holdings_value:
                p.bench_holdings_value[t] *= (1.0 + paths[t][d])
            p.value = sum(holdings.values()) if holdings else p.value
            p.benchmark_value = sum(p.bench_holdings_value.values())
            p.equity_curve.append(p.value)
            p.benchmark_curve.append(p.benchmark_value)
            p.peak_value = max(p.peak_value, p.value)
            if p.peak_value > 0:
                p.max_drawdown = max(p.max_drawdown,
                                     1.0 - p.value / p.peak_value)
        # quarterly benchmark rebalance back to equal weight
        bw = p.benchmark_value / len(self.world.tickers)
        p.bench_holdings_value = {t: bw for t in self.world.tickers}

        p.holdings_value = holdings
        p.weights = ({t: v / p.value for t, v in holdings.items()}
                     if p.value > 0 and holdings else {})
        q_ret = (p.value / value_start - 1.0) if value_start > 0 else 0.0
        return RebalanceReport(
            quarter=q, n_holdings=len(holdings), turnover=turnover,
            transaction_cost=cost, quarter_return=q_ret,
            portfolio_value=p.value, benchmark_value=p.benchmark_value,
            max_drawdown_to_date=p.max_drawdown,
            held_over=held_over, note=note)
