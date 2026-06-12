"""Simulated market + deliberately hostile data API.

Design goals:
  * Deterministic ground truth. Every (ticker, quarter) pair derives its
    fundamentals and daily price path from a hash of (seed, ticker, quarter),
    so retries return identical data and two runs with the same seed produce
    identical economics — which makes the whole pipeline replayable.
  * Non-deterministic-feeling transport. Failures (timeouts, hard errors,
    latency) are seeded per (ticker, quarter, attempt), so a retry can
    legitimately succeed where the first attempt failed, while the run as a
    whole remains reproducible.
  * Structural regime change. One ticker's latent quality decays from a
    scheduled quarter onward, eroding its moat score so the filter agent's
    structural-change detector has something real to catch.
"""
from __future__ import annotations

import asyncio
import hashlib
import math
import random
from typing import Dict, List

from .config import PipelineConfig
from .messages import FundamentalRecord


class ApiError(RuntimeError):
    """Hard transport failure from the simulated vendor."""


def _seeded_rng(*parts) -> random.Random:
    """Stable cross-run RNG. Never use built-in hash() on strings here —
    it is salted per process and would destroy reproducibility."""
    key = ":".join(str(p) for p in parts).encode()
    seed_int = int.from_bytes(hashlib.sha256(key).digest()[:8], "big")
    return random.Random(seed_int)


class SimulatedWorld:
    """Ground truth: latent quality per ticker drives both fundamentals and
    price drift, with a scripted moat-erosion event for one name."""

    def __init__(self, cfg: PipelineConfig) -> None:
        self.cfg = cfg
        rng = _seeded_rng(cfg.seed, "universe")
        self.tickers: List[str] = [self._make_ticker(rng, i)
                                   for i in range(cfg.universe_size)]
        # Latent quality theta in (0,1); Beta(2.2, 2.0) gives a usable
        # high-quality tail without making the screen trivial.
        self.theta0: Dict[str, float] = {
            t: min(0.98, max(0.02, rng.betavariate(2.2, 2.0)))
            for t in self.tickers
        }
        # Script a structural moat erosion: the 3rd-highest-quality name
        # starts decaying at ~40% of the run. Tests assert this is caught.
        ranked = sorted(self.tickers, key=lambda t: self.theta0[t], reverse=True)
        self.erosion_ticker: str = ranked[2]
        self.erosion_start_quarter: int = max(3, int(cfg.quarters * 0.4))
        self.erosion_decay_per_quarter: float = 0.09

    @staticmethod
    def _make_ticker(rng: random.Random, i: int) -> str:
        letters = "BCDFGHJKLMNPQRSTVWXZ"
        return (letters[i % len(letters)]
                + rng.choice("AEIOU")
                + rng.choice(letters)
                + rng.choice(letters))

    # -- latent quality ------------------------------------------------------
    def theta(self, ticker: str, quarter: int) -> float:
        th = self.theta0[ticker]
        if (ticker == self.erosion_ticker
                and quarter >= self.erosion_start_quarter):
            decay = self.erosion_decay_per_quarter * (
                quarter - self.erosion_start_quarter + 1)
            th = max(0.05, th - decay)
        return th

    # -- fundamentals (deterministic per ticker-quarter) ----------------------
    def fundamentals(self, ticker: str, quarter: int) -> FundamentalRecord:
        th = self.theta(ticker, quarter)
        rng = _seeded_rng(self.cfg.seed, "fund", ticker, quarter)
        revenue = 1000.0 * (0.5 + 1.5 * th) * (1.0 + 0.01 * quarter)
        margin = 0.05 + 0.20 * th + rng.gauss(0, 0.015)
        net_income = revenue * margin
        cfo = net_income * (1.0 + 0.10 * th + rng.gauss(0, 0.08))
        # FCF tracks NI tightly for high-theta names (accrual quality)
        fcf = net_income * (1.0 + rng.gauss(0, 0.22 * (1.05 - th)))
        total_assets = revenue * (2.6 - 1.1 * th + rng.gauss(0, 0.10))
        ebitda = max(1e-6, net_income * (1.55 + rng.gauss(0, 0.10)))
        total_debt = max(0.0, ebitda * (4.2 - 4.4 * th + rng.gauss(0, 0.55)))
        cash = max(0.0, ebitda * (0.4 + 0.9 * th + rng.gauss(0, 0.20)))
        equity = total_assets - total_debt - revenue * 0.35
        roic = 0.055 + 0.225 * th + rng.gauss(0, 0.018)
        moat = round(min(8.0, max(0.0, 8.2 * th + rng.gauss(0, 0.55))))
        fcf_yield = min(0.16, max(0.025,
                                  0.045 + 0.075 * rng.random()
                                  + 0.012 * (1 - th)))
        market_cap = max(1e-6, fcf) / fcf_yield
        tilt = 0.32 + 0.62 * th  # P(positive) for each Piotroski-ish delta
        def delta(scale: float, invert: bool = False) -> float:
            sign = 1.0 if rng.random() < tilt else -1.0
            if invert:
                sign = -sign
            return sign * abs(rng.gauss(0, scale))
        return FundamentalRecord(
            ticker=ticker, quarter=quarter,
            roic=roic, net_income=net_income, cfo=cfo, fcf=fcf,
            total_assets=total_assets, total_debt=total_debt, cash=cash,
            ebitda=ebitda, equity=equity, market_cap=market_cap,
            moat_score=float(moat),
            roa_delta=delta(0.01),
            leverage_delta=delta(0.05, invert=True),   # good = decreasing
            margin_delta=delta(0.008),
            asset_turnover_delta=delta(0.04),
            shares_delta=delta(0.01, invert=True),     # good = buybacks
        )

    # -- prices (deterministic per ticker-quarter) -----------------------------
    def quarter_daily_returns(self, ticker: str, quarter: int) -> List[float]:
        th = self.theta(ticker, quarter)
        rng = _seeded_rng(self.cfg.seed, "px", ticker, quarter)
        mu_ann = 0.015 + 0.145 * th
        sigma_ann = 0.36 - 0.16 * th
        mu_d = mu_ann / 252.0
        sigma_d = sigma_ann / math.sqrt(252.0)
        return [mu_d + sigma_d * rng.gauss(0, 1)
                for _ in range(self.cfg.trading_days_per_quarter)]


class FlakyMarketDataApi:
    """Transport wrapper that injects latency, hangs, hard failures, missing
    fields, NaNs, absurd values, and duplicate rows around the world's
    deterministic ground truth."""

    def __init__(self, cfg: PipelineConfig, world: SimulatedWorld) -> None:
        self.cfg = cfg
        self.world = world

    async def fetch_fundamentals(self, ticker: str, quarter: int,
                                 attempt: int) -> List[FundamentalRecord]:
        """Returns a LIST of rows (the vendor occasionally duplicates).
        Raises ApiError on hard failure; hangs (sleeps) past any sane
        timeout on a hang event — callers must wrap with wait_for."""
        prof = self.cfg.api
        rng = _seeded_rng(self.cfg.seed, "net", ticker, quarter, attempt)
        latency = min(0.9, rng.expovariate(1.0 / max(1e-6, prof.mean_latency_s)))
        if rng.random() < prof.hang_rate:
            latency = self.cfg.api_timeout_s * 3.0 + 1.0
        await asyncio.sleep(latency)
        if rng.random() < prof.hard_fail_rate:
            raise ApiError(f"vendor 5xx for {ticker} q{quarter} (attempt {attempt})")

        record = self.world.fundamentals(ticker, quarter)
        # Corruption is seeded per (ticker, quarter) — NOT per attempt — so a
        # retry cannot launder bad content; only transport faults are retryable.
        crng = _seeded_rng(self.cfg.seed, "corrupt", ticker, quarter)
        for f in FundamentalRecord.NUMERIC_FIELDS:
            r = crng.random()
            if r < prof.field_missing_rate:
                setattr(record, f, None)
            elif r < prof.field_missing_rate + prof.nan_rate:
                setattr(record, f, float("nan"))
        if crng.random() < prof.absurd_value_rate:
            record.roic = 4.20  # 420% ROIC — clearly garbage
        rows = [record]
        if crng.random() < prof.duplicate_rate:
            rows.append(record)
        return rows
