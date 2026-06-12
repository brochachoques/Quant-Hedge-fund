"""Externalized pipeline state.

In production this would be Redis/Postgres. Keeping ALL durable agent state
here (last-known-good fundamentals, filter history, portfolio state,
counters) is what makes every agent restart-safe: a recycled agent
reconstructs itself entirely from the store, so the supervisor can kill and
respawn any of them mid-run without corrupting the pipeline.

All methods are synchronous and contain no awaits, so within a single
asyncio event loop each call is atomic — no locks required.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Set, Tuple

from .messages import FundamentalRecord


@dataclass
class PortfolioState:
    value: float = 1.0
    benchmark_value: float = 1.0
    weights: Dict[str, float] = field(default_factory=dict)        # post-drift
    holdings_value: Dict[str, float] = field(default_factory=dict)
    bench_holdings_value: Dict[str, float] = field(default_factory=dict)
    equity_curve: List[float] = field(default_factory=lambda: [1.0])
    benchmark_curve: List[float] = field(default_factory=lambda: [1.0])
    peak_value: float = 1.0
    max_drawdown: float = 0.0
    total_costs: float = 0.0
    last_quarter_processed: int = -1


class DataStore:
    def __init__(self) -> None:
        # ticker -> (record, quarter_observed)
        self._lkg: Dict[str, Tuple[FundamentalRecord, int]] = {}
        # ticker -> deque[(quarter, moat_score, roic)]
        self._history: Dict[str, Deque[Tuple[int, float, float]]] = defaultdict(
            lambda: deque(maxlen=12)
        )
        self.portfolio = PortfolioState()
        self.counters: Dict[str, int] = defaultdict(int)
        self.flags: Set[str] = set()
        self.last_filter_quarter: int = -1
        self.quarter_reports: List[dict] = []
        self.created_at = time.time()

    # -- last-known-good cache ---------------------------------------------
    def update_lkg(self, record: FundamentalRecord) -> None:
        prev = self._lkg.get(record.ticker)
        if prev is None or record.quarter >= prev[1]:
            self._lkg[record.ticker] = (record, record.quarter)

    def get_lkg(self, ticker: str) -> Optional[Tuple[FundamentalRecord, int]]:
        return self._lkg.get(ticker)

    # -- filter history (moat erosion detection survives restarts) ----------
    def push_history(self, ticker: str, quarter: int,
                     moat: float, roic: float) -> None:
        h = self._history[ticker]
        if h and h[-1][0] == quarter:        # idempotent on re-ticked quarters
            h[-1] = (quarter, moat, roic)
        else:
            h.append((quarter, moat, roic))

    def get_history(self, ticker: str) -> List[Tuple[int, float, float]]:
        return list(self._history[ticker])

    # -- counters / flags ----------------------------------------------------
    def incr(self, key: str, by: int = 1) -> None:
        self.counters[key] += by

    def snapshot_counters(self) -> Dict[str, int]:
        return dict(self.counters)

    def set_flag(self, flag: str) -> bool:
        """Returns True if the flag was newly set (atomic test-and-set)."""
        if flag in self.flags:
            return False
        self.flags.add(flag)
        return True
