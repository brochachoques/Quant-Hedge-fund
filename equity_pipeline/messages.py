"""Typed message contracts. Everything that crosses a queue is defined here."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Domain records
# ---------------------------------------------------------------------------

@dataclass
class FundamentalRecord:
    """One company-quarter of fundamentals as delivered by the (flaky) API.

    Any numeric field may arrive as None or NaN; the filter agent owns
    validation and imputation. `staleness` > 0 marks a last-known-good
    substitute injected by the self-correction agent.
    """

    ticker: str
    quarter: int
    roic: Optional[float] = None
    net_income: Optional[float] = None
    cfo: Optional[float] = None                # cash flow from operations
    fcf: Optional[float] = None
    total_assets: Optional[float] = None
    total_debt: Optional[float] = None
    cash: Optional[float] = None
    ebitda: Optional[float] = None
    equity: Optional[float] = None
    market_cap: Optional[float] = None
    moat_score: Optional[float] = None          # qualitative 0-8
    # year-over-year deltas used by the Piotroski-style composite
    roa_delta: Optional[float] = None
    leverage_delta: Optional[float] = None
    margin_delta: Optional[float] = None
    asset_turnover_delta: Optional[float] = None
    shares_delta: Optional[float] = None
    staleness: int = 0
    anomalies: List[str] = field(default_factory=list)

    NUMERIC_FIELDS = (
        "roic", "net_income", "cfo", "fcf", "total_assets", "total_debt",
        "cash", "ebitda", "equity", "market_cap", "moat_score",
        "roa_delta", "leverage_delta", "margin_delta",
        "asset_turnover_delta", "shares_delta",
    )


class AnomalyKind(str, Enum):
    MISSING_FIELD = "MISSING_FIELD"
    NAN_VALUE = "NAN_VALUE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    NEGATIVE_EQUITY = "NEGATIVE_EQUITY"
    STALE_SUBSTITUTE = "STALE_SUBSTITUTE"
    DUPLICATE_ROW = "DUPLICATE_ROW"
    IMPUTED = "IMPUTED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    MOAT_EROSION = "MOAT_EROSION"


# ---------------------------------------------------------------------------
# Pipeline messages
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QuarterTick:
    """Supervisor -> Ingestion: begin work on a simulated quarter."""
    quarter: int
    attempt: int = 1


@dataclass
class RawBatch:
    """Ingestion -> Filter: everything fetched this quarter.

    `pending` lists tickers handed to the self-correction agent as dead
    letters; the filter holds the quarter open (bounded by patch_window_s)
    until each one is patched or formally excluded.
    """
    quarter: int
    attempt: int
    records: List[FundamentalRecord]
    pending: List[str]
    api_calls: int = 0
    api_failures: int = 0


@dataclass
class DeadLetter:
    """Ingestion -> Sentinel: a ticker the primary path could not fetch."""
    ticker: str
    quarter: int
    reason: str
    attempts_made: int


@dataclass
class PatchRecord:
    """Sentinel -> Filter: resolution of a dead letter.

    record is None + excluded=True when recovery permanently failed and no
    acceptable last-known-good exists; the filter must not wait further.
    """
    ticker: str
    quarter: int
    record: Optional[FundamentalRecord]
    excluded: bool = False
    reason: str = ""


@dataclass
class LayerResult:
    name: str
    passed: bool
    detail: str


@dataclass
class FilterVerdict:
    ticker: str
    quarter: int
    passed: bool
    composite_score: float
    layers: List[LayerResult] = field(default_factory=list)
    anomalies: List[str] = field(default_factory=list)
    rejection_reasons: List[str] = field(default_factory=list)


@dataclass
class FilteredBatch:
    """Filter -> Backtest: the screened universe for one quarter."""
    quarter: int
    approved: List[FilterVerdict]
    rejected: List[FilterVerdict]
    excluded_tickers: List[str]
    anomaly_count: int


@dataclass
class RebalanceReport:
    """Backtest -> Supervisor: results of one quarterly rebalance + sim."""
    quarter: int
    n_holdings: int
    turnover: float
    transaction_cost: float
    quarter_return: float
    portfolio_value: float
    benchmark_value: float
    max_drawdown_to_date: float
    held_over: bool = False
    note: str = ""


# ---------------------------------------------------------------------------
# Control-plane messages
# ---------------------------------------------------------------------------

class HealthKind(str, Enum):
    HEARTBEAT = "HEARTBEAT"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    RESTART = "RESTART"


@dataclass
class HealthEvent:
    agent: str
    kind: HealthKind
    ts: float = field(default_factory=time.time)
    payload: Dict = field(default_factory=dict)


@dataclass
class RestartRequest:
    """Sentinel -> Supervisor: an agent looks dead or hung; recycle it."""
    agent: str
    reason: str


@dataclass(frozen=True)
class Shutdown:
    reason: str = "normal"


class FatalAgentError(RuntimeError):
    """Raised when an agent must crash (propagates out of its task so the
    supervisor's restart machinery engages). Used by fault injection."""
