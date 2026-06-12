"""BaseAgent: the actor skeleton every pipeline agent inherits.

Responsibilities:
  * single-inbox message loop with graceful Shutdown handling
  * background heartbeat emission to the health queue
  * error containment policy: ordinary exceptions in handle() are logged and
    reported but do NOT kill the agent; FatalAgentError (and CancelledError)
    propagate so the supervisor's restart machinery engages.
"""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from ..config import PipelineConfig
from ..messages import FatalAgentError, HealthEvent, HealthKind, Shutdown
from ..store import DataStore


class BaseAgent(ABC):
    def __init__(self, name: str, cfg: PipelineConfig, store: DataStore,
                 inbox: "asyncio.Queue[Any]",
                 health_q: "asyncio.Queue[HealthEvent]") -> None:
        self.name = name
        self.cfg = cfg
        self.store = store
        self.inbox = inbox
        self.health_q = health_q
        self.log = logging.getLogger(f"pipeline.{name}")
        self._hb_task: asyncio.Task | None = None
        self._stopping = False

    # -- lifecycle hooks -----------------------------------------------------
    async def on_start(self) -> None:   # noqa: B027 - intentional no-op hook
        pass

    async def on_stop(self) -> None:    # noqa: B027
        pass

    async def on_shutdown_msg(self, msg: Shutdown) -> None:  # noqa: B027
        """Called when a Shutdown message is received, before the loop exits.
        Agents that must flush final output override this."""

    @abstractmethod
    async def handle(self, msg: Any) -> None:
        ...

    # -- health --------------------------------------------------------------
    def emit_health(self, kind: HealthKind, **payload: Any) -> None:
        try:
            self.health_q.put_nowait(
                HealthEvent(agent=self.name, kind=kind,
                            ts=time.time(), payload=payload))
        except asyncio.QueueFull:       # unbounded by default; belt-and-braces
            self.log.warning("health queue full; dropping %s event", kind)

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                self.emit_health(HealthKind.HEARTBEAT)
                await asyncio.sleep(self.cfg.heartbeat_interval_s)
        except asyncio.CancelledError:
            pass

    # -- main loop -------------------------------------------------------------
    async def run(self) -> None:
        self._hb_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.name}-heartbeat")
        self.log.debug("started")
        self.emit_health(HealthKind.INFO, event="started")
        try:
            await self.on_start()
            while not self._stopping:
                msg = await self.inbox.get()
                if isinstance(msg, Shutdown):
                    self.log.debug("shutdown received (%s)", msg.reason)
                    await self.on_shutdown_msg(msg)
                    break
                try:
                    await self.handle(msg)
                except asyncio.CancelledError:
                    raise
                except FatalAgentError:
                    self.emit_health(HealthKind.ERROR, fatal=True,
                                     msg_type=type(msg).__name__)
                    raise
                except Exception as exc:  # contained: log, report, continue
                    self.store.incr(f"errors:{self.name}")
                    self.log.exception("error handling %s: %s",
                                       type(msg).__name__, exc)
                    self.emit_health(HealthKind.ERROR, fatal=False,
                                     error=str(exc),
                                     msg_type=type(msg).__name__)
        finally:
            if self._hb_task:
                self._hb_task.cancel()
                try:
                    await self._hb_task
                except asyncio.CancelledError:
                    pass
            try:
                await self.on_stop()
            except Exception:           # never let teardown mask the cause
                self.log.exception("error in on_stop")
            self.log.debug("stopped")

    def request_stop(self) -> None:
        self._stopping = True
