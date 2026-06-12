"""Agent 4 — Self-Correction & Logging (the sentinel).

Runs three concurrent internal loops plus a control inbox:

  health loop     — consumes every HealthEvent from every agent; maintains
                    last-seen heartbeat timestamps; writes non-heartbeat
                    events to a structured JSONL log.
  recovery loop   — consumes DeadLetters from ingestion and re-routes each
                    one through a patient secondary fetch path (longer
                    timeout, fresh retry budget, exponential backoff with
                    jitter, bounded worker pool). On success: updates the
                    last-known-good cache and patches the filter. On
                    permanent failure: sends an explicit exclusion patch so
                    the filter never blocks on a lost ticker.
  monitor loop    — watches heartbeat staleness; if an agent goes silent
                    past the threshold, sends the supervisor a
                    RestartRequest (rate-limited per agent so a flapping
                    agent cannot trigger a restart storm).

This agent intentionally does NOT inherit the single-inbox BaseAgent loop —
it is the one component whose job is to multiplex several streams — but it
still emits its own lifecycle events and honors Shutdown.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Dict, Optional

from ..config import PipelineConfig
from ..marketsim import ApiError, FlakyMarketDataApi
from ..messages import (DeadLetter, HealthEvent, HealthKind, PatchRecord,
                        RestartRequest, Shutdown)
from ..store import DataStore


class SelfCorrectionAgent:
    name = "sentinel"

    def __init__(self, cfg: PipelineConfig, store: DataStore,
                 inbox: asyncio.Queue, health_q: asyncio.Queue,
                 deadletter_q: asyncio.Queue, patch_q: asyncio.Queue,
                 control_q: asyncio.Queue, api: FlakyMarketDataApi) -> None:
        self.cfg = cfg
        self.store = store
        self.inbox = inbox
        self.health_q = health_q
        self.deadletter_q = deadletter_q
        self.patch_q = patch_q
        self.control_q = control_q
        self.api = api
        self.log = logging.getLogger("pipeline.sentinel")
        self.last_seen: Dict[str, float] = {}
        self.last_restart_request: Dict[str, float] = {}
        self._stop = asyncio.Event()
        self._recovery_sem = asyncio.Semaphore(cfg.sentinel_parallelism)
        self._recovery_tasks: set = set()
        self._event_fh = None
        self._rng = random.Random(cfg.seed ^ 0xC0FFEE)  # jitter only

    # ------------------------------------------------------------------ run
    async def run(self) -> None:
        if self.cfg.event_log_path:
            try:
                self._event_fh = open(self.cfg.event_log_path, "a",
                                      encoding="utf-8")
            except OSError as e:
                self.log.warning("cannot open event log %s: %s",
                                 self.cfg.event_log_path, e)
        self.log.debug("started")
        loops = [
            asyncio.create_task(self._health_loop(), name="sentinel-health"),
            asyncio.create_task(self._recovery_loop(), name="sentinel-recovery"),
            asyncio.create_task(self._monitor_loop(), name="sentinel-monitor"),
            asyncio.create_task(self._inbox_loop(), name="sentinel-inbox"),
        ]
        try:
            await self._stop.wait()
        finally:
            for t in loops:
                t.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            if self._recovery_tasks:
                await asyncio.gather(*self._recovery_tasks,
                                     return_exceptions=True)
            if self._event_fh:
                self._event_fh.close()
            self.log.debug("stopped")

    async def _inbox_loop(self) -> None:
        while True:
            msg = await self.inbox.get()
            if isinstance(msg, Shutdown):
                self.log.debug("shutdown received (%s)", msg.reason)
                self._stop.set()
                return

    # ------------------------------------------------------- health + logging
    def _write_event(self, ev: HealthEvent) -> None:
        if not self._event_fh:
            return
        try:
            self._event_fh.write(json.dumps({
                "ts": round(ev.ts, 3), "agent": ev.agent,
                "kind": ev.kind.value, **ev.payload}) + "\n")
        except (OSError, TypeError, ValueError) as e:
            self.log.warning("event log write failed: %s", e)

    async def _health_loop(self) -> None:
        while True:
            ev: HealthEvent = await self.health_q.get()
            if ev.kind == HealthKind.HEARTBEAT:
                # Liveness is keyed off heartbeats only: one-off events from
                # non-heartbeating senders (e.g. the supervisor's own RESTART
                # notices) must not enroll them in stale-detection.
                self.last_seen[ev.agent] = ev.ts
                continue                       # too chatty for the JSONL log
            if ev.agent in self.last_seen:
                self.last_seen[ev.agent] = ev.ts
            self._write_event(ev)
            if ev.kind == HealthKind.ERROR:
                self.store.incr(f"health_errors:{ev.agent}")
                lvl = logging.ERROR if ev.payload.get("fatal") else logging.WARNING
                self.log.log(lvl, "health ERROR from %s: %s", ev.agent,
                             ev.payload)

    # ------------------------------------------------------------- monitoring
    async def _monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.heartbeat_interval_s)
            now = time.time()
            for agent, seen in list(self.last_seen.items()):
                age = now - seen
                if age <= self.cfg.heartbeat_stale_s:
                    continue
                last_req = self.last_restart_request.get(agent, 0.0)
                if now - last_req < self.cfg.restart_request_cooldown_s:
                    continue
                self.last_restart_request[agent] = now
                self.store.incr("restart_requests")
                self.log.error("agent %s heartbeat stale (%.1fs) — "
                               "requesting restart", agent, age)
                self._write_event(HealthEvent(
                    agent=self.name, kind=HealthKind.RESTART,
                    payload={"target": agent, "stale_s": round(age, 2)}))
                await self.control_q.put(RestartRequest(
                    agent=agent, reason=f"heartbeat stale {age:.1f}s"))

    # --------------------------------------------------------------- recovery
    async def _recovery_loop(self) -> None:
        while True:
            dl: DeadLetter = await self.deadletter_q.get()
            task = asyncio.create_task(
                self._recover(dl), name=f"recover-{dl.ticker}-q{dl.quarter}")
            self._recovery_tasks.add(task)
            task.add_done_callback(self._recovery_tasks.discard)

    async def _recover(self, dl: DeadLetter) -> None:
        """Patient secondary route: fresh attempt budget, longer timeout,
        exponential backoff with jitter."""
        async with self._recovery_sem:
            self.log.info("recovering %s q%d (primary failed: %s)",
                          dl.ticker, dl.quarter, dl.reason)
            last_err: Optional[str] = None
            for k in range(1, self.cfg.sentinel_attempts + 1):
                # offset the attempt index so the seeded transport stream
                # differs from ingestion's exhausted attempts
                attempt = 100 + dl.attempts_made + k
                try:
                    rows = await asyncio.wait_for(
                        self.api.fetch_fundamentals(dl.ticker, dl.quarter,
                                                    attempt),
                        timeout=self.cfg.sentinel_timeout_s)
                    record = rows[0]
                    self.store.update_lkg(record)
                    self.store.incr("dl_recovered")
                    self.log.info("recovered %s q%d on secondary attempt %d",
                                  dl.ticker, dl.quarter, k)
                    self._write_event(HealthEvent(
                        agent=self.name, kind=HealthKind.INFO,
                        payload={"event": "dl_recovered",
                                 "ticker": dl.ticker, "quarter": dl.quarter,
                                 "attempt": k}))
                    await self.patch_q.put(PatchRecord(
                        ticker=dl.ticker, quarter=dl.quarter, record=record))
                    return
                except (asyncio.TimeoutError, ApiError) as e:
                    last_err = (f"timeout after {self.cfg.sentinel_timeout_s}s"
                                if isinstance(e, asyncio.TimeoutError)
                                else str(e))
                    backoff = (self.cfg.sentinel_backoff_s * (2 ** (k - 1))
                               * (1.0 + 0.3 * self._rng.random()))
                    await asyncio.sleep(backoff)
            self.store.incr("dl_exhausted")
            self.log.warning("recovery exhausted for %s q%d (%s) — "
                             "issuing exclusion patch", dl.ticker, dl.quarter,
                             last_err)
            self._write_event(HealthEvent(
                agent=self.name, kind=HealthKind.WARN,
                payload={"event": "dl_exhausted", "ticker": dl.ticker,
                         "quarter": dl.quarter, "last_error": last_err}))
            await self.patch_q.put(PatchRecord(
                ticker=dl.ticker, quarter=dl.quarter, record=None,
                excluded=True,
                reason=f"secondary route exhausted: {last_err}"))
