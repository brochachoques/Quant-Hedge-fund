"""Central configuration. Every tunable knob for the pipeline lives here."""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class QualityThresholds:
    """Five-layer quality screen + valuation guardrail.

    Defaults mirror a Buffett/Munger-style compounder screen:
      L1  ROIC >= 15%
      L2  FCF conversion gap (|FCF/NI - 1|) < 20%
      L3  Piotroski-style composite >= 6 of 9
      L4  Net Debt / EBITDA < 3.0x
      L5  Qualitative moat score >= 5 of 8, with no structural erosion
      VG  FCF yield inside [min, max] band

    NOTE: the live-screen band is typically 6-10%; the simulated default is
    widened to 5-12% so the synthetic universe retains breadth. Tighten via
    config if you want the strict band.
    """

    min_roic: float = 0.15
    max_fcf_conversion_gap: float = 0.20
    min_piotroski: int = 6
    max_net_debt_ebitda: float = 3.0
    min_moat_score: int = 5
    fcf_yield_band: Tuple[float, float] = (0.05, 0.12)
    moat_erosion_points: int = 2          # drop vs trailing max => structural flag
    moat_erosion_lookback: int = 6        # quarters of history inspected


@dataclass
class ApiFailureProfile:
    """How hostile the simulated market-data transport is."""

    mean_latency_s: float = 0.06
    hard_fail_rate: float = 0.12          # raises ApiError
    hang_rate: float = 0.05               # sleeps past the caller's timeout
    field_missing_rate: float = 0.04      # a numeric field arrives as None
    nan_rate: float = 0.02                # a numeric field arrives as NaN
    absurd_value_rate: float = 0.01       # e.g. ROIC of 420%
    duplicate_rate: float = 0.03          # API returns the same row twice


@dataclass
class PipelineConfig:
    # --- simulation scope -------------------------------------------------
    quarters: int = 12
    universe_size: int = 24
    trading_days_per_quarter: int = 63
    seed: int = 11

    # --- transport / ingestion -------------------------------------------
    api: ApiFailureProfile = field(default_factory=ApiFailureProfile)
    api_timeout_s: float = 1.0
    ingest_attempts: int = 2              # ingestion's own retries before dead-letter
    ingest_backoff_s: float = 0.15
    max_parallel_fetches: int = 12

    # circuit breaker around the primary fetch path
    breaker_window: int = 30
    breaker_open_failure_ratio: float = 0.60
    breaker_min_calls: int = 12
    breaker_cooldown_s: float = 2.0
    breaker_half_open_probes: int = 3

    # --- self-correction (sentinel) ---------------------------------------
    sentinel_attempts: int = 3            # recovery retries per dead letter
    sentinel_backoff_s: float = 0.25
    sentinel_timeout_s: float = 2.0       # patient secondary route
    sentinel_parallelism: int = 4
    patch_window_s: float = 6.0           # filter waits this long for patches
    max_staleness_quarters: int = 2       # last-known-good acceptability
    heartbeat_interval_s: float = 0.4
    heartbeat_stale_s: float = 2.5
    restart_request_cooldown_s: float = 5.0

    # --- supervisor --------------------------------------------------------
    quarter_timeout_s: float = 25.0
    quarter_retries: int = 1              # re-tick a quarter that timed out
    max_restarts_per_agent: int = 3
    shutdown_grace_s: float = 5.0

    # --- quality screen ----------------------------------------------------
    thresholds: QualityThresholds = field(default_factory=QualityThresholds)

    # --- backtest ----------------------------------------------------------
    transaction_cost_bps: float = 10.0
    weighting: str = "equal"              # "equal" | "score"
    max_position_weight: float = 0.30
    risk_free_rate: float = 0.0

    # --- fault injection (used by the stress harness) ----------------------
    # e.g. {"filter": 3} => FilterAgent raises a fatal error once, in quarter 3
    inject_crash: Dict[str, int] = field(default_factory=dict)

    # --- logging -----------------------------------------------------------
    log_level: str = "INFO"
    event_log_path: Optional[str] = "pipeline_events.jsonl"

    @staticmethod
    def from_args(argv: Optional[list] = None) -> "PipelineConfig":
        p = argparse.ArgumentParser(description="Multi-agent equity pipeline")
        p.add_argument("--quarters", type=int, default=12)
        p.add_argument("--universe", type=int, default=24)
        p.add_argument("--seed", type=int, default=11)
        p.add_argument("--log-level", type=str, default="INFO")
        p.add_argument("--weighting", choices=["equal", "score"], default="equal")
        p.add_argument("--hard-fail-rate", type=float, default=None,
                       help="override simulated API hard-failure rate")
        p.add_argument("--hang-rate", type=float, default=None,
                       help="override simulated API hang rate")
        p.add_argument("--event-log", type=str, default="pipeline_events.jsonl")
        a = p.parse_args(argv)
        cfg = PipelineConfig(
            quarters=a.quarters,
            universe_size=a.universe,
            seed=a.seed,
            log_level=a.log_level,
            weighting=a.weighting,
            event_log_path=a.event_log,
        )
        if a.hard_fail_rate is not None:
            cfg.api.hard_fail_rate = a.hard_fail_rate
        if a.hang_rate is not None:
            cfg.api.hang_rate = a.hang_rate
        return cfg
