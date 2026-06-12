"""Agent 1 — Ingestion.

Pulls the full universe for a quarter concurrently against the flaky API:
  * bounded parallelism (semaphore)
  * per-ticker timeout + bounded internal retries with backoff
  * a circuit breaker around the primary path: when the vendor is melting
    down, stop hammering it, fast-fail remaining tickers to the dead-letter
    queue and let the self-correction agent recover them on its patient
    secondary route
  * dedupes vendor duplicate rows, forwards a RawBatch with an explicit
    `pending` list so the filter knows exactly what is still in flight.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Tuple

from ..config import PipelineConfig
from ..marketsim import ApiError, FlakyMarketDataApi
from ..messages import (AnomalyKind, DeadLetter, FundamentalRecord,
                        HealthKind, QuarterTick, RawBatch)
from ..store import DataStore
from .base import BaseAgent


class CircuitBreaker:
    """Rolling-window breaker: CLOSED -> OPEN (cooldown) -> HALF_OPEN probes."""

    CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"

    def __init__(self, window: int, open_failure_ratio: float,
                 min_calls: int, cooldown_s: float, half_open_probes: int):
        self.window = window
        self.open_failure_ratio = open_failure_ratio
        self.min_calls = min_calls
        self.cooldown_s = cooldown_s
        self.half_open_probes = half_open_probes
        self.state = self.CLOSED
        self._results: List[bool] = []
        self._opened_at = 0.0
        self._probes_left = 0
        self.times_opened = 0

    def allow(self) -> bool:
        if self.state == self.OPEN:
            if time.monotonic() - self._opened_at >= self.cooldown_s:
                self.state = self.HALF_OPEN
                self._probes_left = self.half_open_probes
                return True
            return False
        if self.state == self.HALF_OPEN:
            if self._probes_left > 0:
                self._probes_left -= 1
                return True
            return False
        return True

    def record(self, success: bool) -> None:
        if self.state == self.HALF_OPEN:
            if success:
                self.state = self.CLOSED
                self._results = []
            else:
                self._trip()
            return
        self._results.append(success)
        if len(self._results) > self.window:
            self._results.pop(0)
        if len(self._results) >= self.min_calls:
            fail_ratio = self._results.count(False) / len(self._results)
            if fail_ratio >= self.open_failure_ratio:
                self._trip()

    def _trip(self) -> None:
        self.state = self.OPEN
        self._opened_at = time.monotonic()
        self._results = []
        self.times_opened += 1


class IngestionAgent(BaseAgent):
    def __init__(self, cfg: PipelineConfig, store: DataStore,
                 inbox: asyncio.Queue, health_q: asyncio.Queue,
                 api: FlakyMarketDataApi,
                 raw_out: asyncio.Queue, deadletter_q: asyncio.Queue,
                 universe: List[str]) -> None:
        super().__init__("ingestion", cfg, store, inbox, health_q)
        self.api = api
        self.raw_out = raw_out
        self.deadletter_q = deadletter_q
        self.universe = sorted(universe)   # deterministic iteration order
        self.breaker = CircuitBreaker(
            cfg.breaker_window, cfg.breaker_open_failure_ratio,
            cfg.breaker_min_calls, cfg.breaker_cooldown_s,
            cfg.breaker_half_open_probes)

    async def handle(self, msg: Any) -> None:
        if not isinstance(msg, QuarterTick):
            self.log.warning("ignoring unexpected message %s", type(msg).__name__)
            return
        await self._ingest_quarter(msg)

    async def _ingest_quarter(self, tick: QuarterTick) -> None:
        q = tick.quarter
        if tick.attempt > 1:
            self.log.warning("re-ingesting quarter %d (attempt %d)",
                             q, tick.attempt)
        sem = asyncio.Semaphore(self.cfg.max_parallel_fetches)
        api_calls = 0
        api_failures = 0

        async def fetch_one(ticker: str) -> Tuple[str, str, Any]:
            nonlocal api_calls, api_failures
            last_err = "unknown"
            for attempt in range(1, self.cfg.ingest_attempts + 1):
                if not self.breaker.allow():
                    self.store.incr("breaker_fast_fails")
                    return ("deadletter", ticker, "circuit breaker OPEN")
                async with sem:
                    api_calls += 1
                    try:
                        rows = await asyncio.wait_for(
                            self.api.fetch_fundamentals(ticker, q, attempt),
                            timeout=self.cfg.api_timeout_s)
                        self.breaker.record(True)
                        return ("ok", ticker, rows)
                    except asyncio.TimeoutError:
                        last_err = f"timeout after {self.cfg.api_timeout_s}s"
                    except ApiError as e:
                        last_err = str(e)
                    api_failures += 1
                    self.breaker.record(False)
                await asyncio.sleep(self.cfg.ingest_backoff_s * attempt)
            return ("deadletter", ticker, last_err)

        results = await asyncio.gather(*(fetch_one(t) for t in self.universe))

        records: Dict[str, FundamentalRecord] = {}
        pending: List[str] = []
        for status, ticker, payload in results:
            if status == "ok":
                rows: List[FundamentalRecord] = payload
                if len(rows) > 1:
                    self.store.incr("duplicate_rows_dropped", len(rows) - 1)
                    rows[0].anomalies.append(AnomalyKind.DUPLICATE_ROW.value)
                records[ticker] = rows[0]
            else:
                pending.append(ticker)
                self.store.incr("dead_letters")
                await self.deadletter_q.put(DeadLetter(
                    ticker=ticker, quarter=q, reason=str(payload),
                    attempts_made=self.cfg.ingest_attempts))

        if self.breaker.state != CircuitBreaker.CLOSED:
            self.emit_health(HealthKind.WARN, event="circuit_breaker",
                             state=self.breaker.state, quarter=q)
            self.log.warning("circuit breaker is %s (opened %d times so far)",
                             self.breaker.state, self.breaker.times_opened)

        batch = RawBatch(
            quarter=q, attempt=tick.attempt,
            records=sorted(records.values(), key=lambda r: r.ticker),
            pending=sorted(pending),
            api_calls=api_calls, api_failures=api_failures)
        self.log.info(
            "q%-2d fetched %d/%d tickers (%d dead-lettered, %d api calls, "
            "%d transport failures)", q, len(records), len(self.universe),
            len(pending), api_calls, api_failures)
        self.emit_health(HealthKind.INFO, event="raw_batch", quarter=q,
                         fetched=len(records), pending=len(pending))
        await self.raw_out.put(batch)
