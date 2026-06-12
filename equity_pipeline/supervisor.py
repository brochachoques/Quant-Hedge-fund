"""The Supervisor: owns the wiring and the lifecycle of all four agents.

  * builds the queue topology and shared services (store, world, flaky API)
  * spawns each agent as a named asyncio task with a done-callback that
    auto-restarts crashed agents (bounded by max_restarts_per_agent)
  * services RestartRequests from the self-correction agent (hung agents),
    with a per-agent lock so the two restart paths can never double-fire
  * drives the simulation clock: emits one QuarterTick at a time and waits
    for the corresponding RebalanceReport; a quarter that times out (e.g.
    its in-flight message died with a crashed agent) is re-ticked once —
    every downstream stage is idempotent, so this is safe
  * performs ordered graceful shutdown and renders the final report.

Queue topology:
    supervisor --ticks-->   ingestion --raw-->    filter --filtered--> backtest
    ingestion  --deadletters--> sentinel --patches--> filter
    backtest   --reports--> supervisor
    everyone   --health-->  sentinel --restart requests--> supervisor
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from . import analytics
from .agents import (AnalyticalFilterAgent, BacktestExecutionAgent,
                     IngestionAgent, SelfCorrectionAgent)
from .config import PipelineConfig
from .marketsim import FlakyMarketDataApi, SimulatedWorld
from .messages import (HealthEvent, HealthKind, QuarterTick, RebalanceReport,
                       RestartRequest, Shutdown)
from .store import DataStore

log = logging.getLogger("pipeline.supervisor")


@dataclass
class RunResult:
    summary: Optional[analytics.PerformanceSummary]
    reports: List[RebalanceReport]
    counters: Dict[str, int]
    restarts: Dict[str, int]
    store: DataStore = field(repr=False, default=None)
    quarters_requested: int = 0

    @property
    def quarters_completed(self) -> int:
        return len(self.reports)


class Supervisor:
    AGENT_KEYS = ("ingestion", "filter", "backtest", "sentinel")

    def __init__(self, cfg: PipelineConfig) -> None:
        self.cfg = cfg
        self.store = DataStore()
        self.world = SimulatedWorld(cfg)
        self.api = FlakyMarketDataApi(cfg, self.world)

        self.q_ticks: asyncio.Queue = asyncio.Queue()
        self.q_raw: asyncio.Queue = asyncio.Queue()
        self.q_patch: asyncio.Queue = asyncio.Queue()
        self.q_filtered: asyncio.Queue = asyncio.Queue()
        self.q_results: asyncio.Queue = asyncio.Queue()
        self.q_deadletter: asyncio.Queue = asyncio.Queue()
        self.q_health: asyncio.Queue = asyncio.Queue()
        self.q_control: asyncio.Queue = asyncio.Queue()

        self.factories: Dict[str, Callable[[], object]] = {
            "ingestion": lambda: IngestionAgent(
                cfg, self.store, self.q_ticks, self.q_health, self.api,
                raw_out=self.q_raw, deadletter_q=self.q_deadletter,
                universe=self.world.tickers),
            "filter": lambda: AnalyticalFilterAgent(
                cfg, self.store, self.q_raw, self.q_health,
                patch_q=self.q_patch, filtered_out=self.q_filtered),
            "backtest": lambda: BacktestExecutionAgent(
                cfg, self.store, self.q_filtered, self.q_health,
                world=self.world, results_out=self.q_results),
            "sentinel": lambda: SelfCorrectionAgent(
                cfg, self.store, inbox=asyncio.Queue(),
                health_q=self.q_health, deadletter_q=self.q_deadletter,
                patch_q=self.q_patch, control_q=self.q_control,
                api=self.api),
        }
        self.agents: Dict[str, object] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.restarts: Dict[str, int] = {k: 0 for k in self.AGENT_KEYS}
        self._restart_locks: Dict[str, asyncio.Lock] = {
            k: asyncio.Lock() for k in self.AGENT_KEYS}
        self._shutting_down = False
        self._reports: Dict[int, RebalanceReport] = {}

    # ------------------------------------------------------------- lifecycle
    def _spawn(self, key: str) -> None:
        agent = self.factories[key]()
        self.agents[key] = agent
        task = asyncio.create_task(agent.run(), name=f"agent-{key}")
        self.tasks[key] = task
        task.add_done_callback(
            lambda t, k=key: asyncio.get_running_loop().create_task(
                self._on_task_done(k, t)))
        log.debug("spawned %s", key)

    async def _on_task_done(self, key: str, task: asyncio.Task) -> None:
        if self._shutting_down or task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return                                   # clean exit
        log.error("agent %s CRASHED: %s", key, exc)
        self.q_health.put_nowait(HealthEvent(
            agent="supervisor", kind=HealthKind.RESTART,
            payload={"target": key, "cause": str(exc)}))
        await self._restart(key, f"crashed: {exc}")

    async def _restart(self, key: str, reason: str) -> None:
        async with self._restart_locks[key]:
            if self._shutting_down:
                return
            if self.restarts[key] >= self.cfg.max_restarts_per_agent:
                log.critical("agent %s exceeded restart budget (%d); "
                             "leaving it down", key, self.restarts[key])
                return
            current = self.tasks.get(key)
            if current is not None and not current.done():
                current.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await current
            self.restarts[key] += 1
            self.store.incr(f"restarts:{key}")
            log.warning("restarting agent %s (#%d) — %s",
                        key, self.restarts[key], reason)
            self._spawn(key)

    async def _control_loop(self) -> None:
        """Service hung-agent restart requests from the self-correction agent."""
        while True:
            req: RestartRequest = await self.q_control.get()
            if req.agent not in self.factories:
                log.warning("restart requested for unknown agent %s", req.agent)
                continue
            task = self.tasks.get(req.agent)
            if task is not None and task.done():
                # the crash path already handled (or is handling) this
                log.debug("restart request for %s ignored; task already done",
                          req.agent)
                continue
            await self._restart(req.agent, f"sentinel: {req.reason}")

    # ------------------------------------------------------------------ run
    async def run(self) -> RunResult:
        log.info("starting pipeline: %d tickers x %d quarters (seed %d)",
                 self.cfg.universe_size, self.cfg.quarters, self.cfg.seed)
        log.info("scripted regime change: %s moat erodes from q%d",
                 self.world.erosion_ticker, self.world.erosion_start_quarter)
        for key in self.AGENT_KEYS:
            self._spawn(key)
        control_task = asyncio.create_task(self._control_loop(),
                                           name="supervisor-control")
        try:
            for q in range(self.cfg.quarters):
                await self._run_quarter(q)
        finally:
            await self._shutdown(control_task)
        return self._build_result()

    async def _run_quarter(self, q: int) -> None:
        loop = asyncio.get_running_loop()
        attempts_left = 1 + self.cfg.quarter_retries
        attempt = 1
        await self.q_ticks.put(QuarterTick(quarter=q, attempt=attempt))
        deadline = loop.time() + self.cfg.quarter_timeout_s
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                attempts_left -= 1
                if attempts_left > 0:
                    attempt += 1
                    log.error("quarter %d timed out after %.1fs — re-ticking "
                              "(attempt %d); downstream stages are idempotent",
                              q, self.cfg.quarter_timeout_s, attempt)
                    self.store.incr("quarter_reticks")
                    await self.q_ticks.put(QuarterTick(quarter=q,
                                                       attempt=attempt))
                    deadline = loop.time() + self.cfg.quarter_timeout_s
                    continue
                log.critical("quarter %d abandoned after %d attempt(s); "
                             "advancing the clock", q, attempt)
                self.store.incr("quarters_abandoned")
                return
            try:
                report: RebalanceReport = await asyncio.wait_for(
                    self.q_results.get(), timeout=remaining)
            except asyncio.TimeoutError:
                continue
            if report.quarter in self._reports:
                continue                            # idempotency backstop
            self._reports[report.quarter] = report
            if report.quarter == q:
                return
            log.info("late report for quarter %d recorded while waiting on "
                     "quarter %d", report.quarter, q)

    async def _shutdown(self, control_task: asyncio.Task) -> None:
        self._shutting_down = True
        log.info("shutting down agents...")
        await self.q_ticks.put(Shutdown())
        await self.q_raw.put(Shutdown())
        await self.q_filtered.put(Shutdown())
        sentinel = self.agents.get("sentinel")
        if sentinel is not None:
            await sentinel.inbox.put(Shutdown())
        pending = [t for t in self.tasks.values() if not t.done()]
        if pending:
            done, still = await asyncio.wait(
                pending, timeout=self.cfg.shutdown_grace_s)
            for t in still:
                log.warning("force-cancelling %s", t.get_name())
                t.cancel()
            await asyncio.gather(*still, return_exceptions=True)
        control_task.cancel()
        with suppress(asyncio.CancelledError):
            await control_task
        # drain any stragglers so nothing is left half-finished
        await asyncio.sleep(0)
        log.info("shutdown complete")

    # ---------------------------------------------------------------- report
    def _build_result(self) -> RunResult:
        p = self.store.portfolio
        reports = [self._reports[k] for k in sorted(self._reports)]
        summary = None
        if len(p.equity_curve) > 1:
            summary = analytics.summarize(
                p.equity_curve, p.benchmark_curve,
                turnovers=[r.turnover for r in reports],
                total_costs=p.total_costs,
                risk_free=self.cfg.risk_free_rate)
        return RunResult(summary=summary, reports=reports,
                         counters=self.store.snapshot_counters(),
                         restarts=dict(self.restarts), store=self.store,
                         quarters_requested=self.cfg.quarters)


def render_final_report(result: RunResult) -> str:
    lines: List[str] = []
    add = lines.append
    add("")
    add("=" * 78)
    add("  QUARTERLY REBALANCE LOG")
    add("=" * 78)
    add(f"  {'Q':>2} {'Names':>5} {'Turnover':>9} {'QtrRet':>8} "
        f"{'NAV':>9} {'Bench':>9} {'MaxDD':>7}  Note")
    for r in result.reports:
        add(f"  {r.quarter:>2} {r.n_holdings:>5} {r.turnover * 100:>8.1f}% "
            f"{r.quarter_return * 100:>+7.2f}% {r.portfolio_value:>9.4f} "
            f"{r.benchmark_value:>9.4f} {r.max_drawdown_to_date * 100:>6.2f}%"
            f"  {r.note}")
    add("")
    add("=" * 78)
    add("  PERFORMANCE SUMMARY (strategy vs equal-weight universe benchmark)")
    add("=" * 78)
    if result.summary:
        lines.extend(result.summary.as_lines())
    else:
        add("  no quarters completed — see counters below")
    add("")
    add("=" * 78)
    add("  RESILIENCE / SELF-CORRECTION COUNTERS")
    add("=" * 78)
    for key in sorted(result.counters):
        add(f"  {key:<32} {result.counters[key]:>6}")
    restarts = {k: v for k, v in result.restarts.items() if v}
    add(f"  {'agent_restarts':<32} {sum(result.restarts.values()):>6}"
        + (f"   {restarts}" if restarts else ""))
    add("=" * 78)
    return "\n".join(lines)
