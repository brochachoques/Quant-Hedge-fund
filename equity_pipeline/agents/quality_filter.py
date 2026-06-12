"""Agent 2 — Analytical Filter.

For each quarter it:
  1. Holds the quarter open for dead-lettered tickers, consuming PatchRecords
     from the self-correction agent up to a hard deadline (patch_window_s);
     anything unresolved falls back to last-known-good (bounded staleness)
     or is excluded.
  2. Validates every record: dedupe flags, None/NaN repair via per-field
     imputation from last-known-good, range sanity (e.g. 420% ROIC), and
     negative-equity flagging (which is exactly why the leverage layer uses
     Net Debt / EBITDA rather than debt-to-equity).
  3. Runs the five-layer quality screen + valuation guardrail and a
     structural moat-erosion detector against rolling history kept in the
     external store (so detection survives agent restarts).

Idempotent per quarter: a re-ticked quarter that was already finalized is
dropped, and history pushes overwrite rather than append.
"""
from __future__ import annotations

import asyncio
import math
from typing import Any, Dict, List, Optional, Tuple

from ..config import PipelineConfig
from ..messages import (AnomalyKind, FatalAgentError, FilteredBatch,
                        FilterVerdict, FundamentalRecord, HealthKind,
                        LayerResult, PatchRecord, RawBatch)
from ..store import DataStore
from .base import BaseAgent

_CRITICAL_FIELDS = (
    "roic", "net_income", "cfo", "fcf", "total_assets", "total_debt",
    "cash", "ebitda", "market_cap", "moat_score",
)
_SOFT_FIELDS = (
    "equity", "roa_delta", "leverage_delta", "margin_delta",
    "asset_turnover_delta", "shares_delta",
)


def _bad(v: Optional[float]) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


class AnalyticalFilterAgent(BaseAgent):
    def __init__(self, cfg: PipelineConfig, store: DataStore,
                 inbox: asyncio.Queue, health_q: asyncio.Queue,
                 patch_q: asyncio.Queue, filtered_out: asyncio.Queue) -> None:
        super().__init__("filter", cfg, store, inbox, health_q)
        self.patch_q = patch_q
        self.filtered_out = filtered_out

    # ------------------------------------------------------------------ main
    async def handle(self, msg: Any) -> None:
        if not isinstance(msg, RawBatch):
            self.log.warning("ignoring unexpected message %s", type(msg).__name__)
            return
        q = msg.quarter

        # Fault injection: crash exactly once, fatally, to exercise the
        # supervisor's restart path. The store flag guarantees "once".
        crash_q = self.cfg.inject_crash.get("filter")
        if crash_q == q and self.store.set_flag(f"crash_injected:filter:{q}"):
            self.log.error("FAULT INJECTION: filter crashing in quarter %d", q)
            raise FatalAgentError(f"injected crash in quarter {q}")

        if q <= self.store.last_filter_quarter:
            self.log.info("q%-2d already finalized; dropping duplicate batch", q)
            return

        records: Dict[str, FundamentalRecord] = {r.ticker: r for r in msg.records}
        excluded: Dict[str, str] = {}
        await self._collect_patches(q, set(msg.pending), records, excluded)

        approved: List[FilterVerdict] = []
        rejected: List[FilterVerdict] = []
        anomaly_count = 0
        for ticker in sorted(records):
            verdict = self._evaluate(records[ticker])
            anomaly_count += len(verdict.anomalies)
            (approved if verdict.passed else rejected).append(verdict)
        anomaly_count += len(excluded)
        self.store.incr("anomalies_flagged", anomaly_count)
        self.store.last_filter_quarter = q

        batch = FilteredBatch(
            quarter=q, approved=approved, rejected=rejected,
            excluded_tickers=sorted(excluded), anomaly_count=anomaly_count)
        self.log.info(
            "q%-2d screened %d names -> %d approved, %d rejected, "
            "%d excluded (no usable data), %d anomalies flagged",
            q, len(records) + len(excluded), len(approved), len(rejected),
            len(excluded), anomaly_count)
        for v in approved:
            self.log.debug("  PASS %s score=%.3f", v.ticker, v.composite_score)
        self.emit_health(HealthKind.INFO, event="filtered_batch", quarter=q,
                         approved=len(approved), rejected=len(rejected),
                         excluded=len(excluded), anomalies=anomaly_count)
        await self.filtered_out.put(batch)

    # -------------------------------------------------------- patch protocol
    async def _collect_patches(self, quarter: int, pending: set,
                               records: Dict[str, FundamentalRecord],
                               excluded: Dict[str, str]) -> None:
        """Consume sentinel patches for this quarter until all pending tickers
        resolve or the patch window closes; then apply LKG fallback."""
        if pending:
            self.log.info("q%-2d holding open for %d pending ticker(s): %s",
                          quarter, len(pending), ", ".join(sorted(pending)))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.cfg.patch_window_s
        while pending:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                patch: PatchRecord = await asyncio.wait_for(
                    self.patch_q.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if patch.quarter != quarter:
                continue                      # stale patch from a prior quarter
            if patch.ticker not in pending:
                continue
            pending.discard(patch.ticker)
            if patch.excluded or patch.record is None:
                excluded[patch.ticker] = patch.reason or "recovery failed"
                self.store.incr("filter_exclusions")
            else:
                records[patch.ticker] = patch.record
        # Patch window closed: last-known-good fallback for stragglers.
        for ticker in sorted(pending):
            lkg = self.store.get_lkg(ticker)
            if lkg and quarter - lkg[1] <= self.cfg.max_staleness_quarters:
                rec, seen_q = lkg
                sub = FundamentalRecord(
                    ticker=rec.ticker, quarter=quarter,
                    **{f: getattr(rec, f)
                       for f in FundamentalRecord.NUMERIC_FIELDS})
                sub.staleness = quarter - seen_q
                sub.anomalies = list(rec.anomalies) + [
                    AnomalyKind.STALE_SUBSTITUTE.value]
                records[ticker] = sub
                self.store.incr("lkg_substitutions")
                self.log.warning("q%-2d using stale data for %s "
                                 "(age %d quarter(s))", quarter, ticker,
                                 sub.staleness)
            else:
                excluded[ticker] = "no patch and no acceptable last-known-good"
                self.store.incr("filter_exclusions")
                self.log.warning("q%-2d excluding %s: %s",
                                 quarter, ticker, excluded[ticker])

    # ---------------------------------------------------- validation + screen
    def _validate(self, rec: FundamentalRecord
                  ) -> Tuple[Optional[FundamentalRecord], List[str]]:
        """Repair or condemn a record. Returns (record|None, anomalies)."""
        anomalies = list(rec.anomalies)
        lkg = self.store.get_lkg(rec.ticker)
        lkg_usable = (lkg is not None and
                      rec.quarter - lkg[1] <= self.cfg.max_staleness_quarters
                      and lkg[1] < rec.quarter)
        for f in (*_CRITICAL_FIELDS, *_SOFT_FIELDS):
            v = getattr(rec, f)
            if not _bad(v):
                continue
            kind = (AnomalyKind.NAN_VALUE if isinstance(v, float)
                    else AnomalyKind.MISSING_FIELD)
            anomalies.append(f"{kind.value}:{f}")
            patched = False
            if lkg_usable:
                lv = getattr(lkg[0], f)
                if not _bad(lv):
                    setattr(rec, f, lv)
                    anomalies.append(f"{AnomalyKind.IMPUTED.value}:{f}")
                    self.store.incr("fields_imputed")
                    patched = True
            if not patched:
                if f in _CRITICAL_FIELDS:
                    anomalies.append(AnomalyKind.INSUFFICIENT_DATA.value)
                    return None, anomalies
                setattr(rec, f, 0.0)   # neutral default for soft fields
        # Range sanity --------------------------------------------------------
        if rec.roic is not None and not (-1.0 <= rec.roic <= 1.5):
            anomalies.append(f"{AnomalyKind.OUT_OF_RANGE.value}:roic={rec.roic:.2f}")
            return None, anomalies
        if rec.market_cap is not None and rec.market_cap <= 0:
            anomalies.append(f"{AnomalyKind.OUT_OF_RANGE.value}:market_cap")
            return None, anomalies
        if rec.equity is not None and rec.equity <= 0:
            # D/E is meaningless here — the screen's leverage layer uses
            # Net Debt / EBITDA precisely so this stays assessable.
            anomalies.append(AnomalyKind.NEGATIVE_EQUITY.value)
        if rec.moat_score is not None:
            rec.moat_score = float(min(8.0, max(0.0, rec.moat_score)))
        # A validated, non-stale record becomes the new last-known-good —
        # the imputation/fallback source for future quarters.
        if rec.staleness == 0:
            self.store.update_lkg(rec)
        return rec, anomalies

    def _piotroski_lite(self, r: FundamentalRecord) -> Tuple[int, str]:
        roa = (r.net_income / r.total_assets) if r.total_assets else 0.0
        checks = [
            ("NI>0", r.net_income > 0),
            ("CFO>0", r.cfo > 0),
            ("ROA>0", roa > 0),
            ("CFO>NI", r.cfo > r.net_income),          # accrual quality
            ("dROA>0", r.roa_delta > 0),
            ("dLev<0", r.leverage_delta < 0),
            ("dMargin>0", r.margin_delta > 0),
            ("dTurns>0", r.asset_turnover_delta > 0),
            ("noDilution", r.shares_delta <= 0),
        ]
        score = sum(1 for _, ok in checks if ok)
        detail = ",".join(name for name, ok in checks if not ok) or "clean"
        return score, f"{score}/9 (missed: {detail})"

    def _moat_erosion(self, ticker: str, current_moat: float) -> Optional[str]:
        """Structural (persistent) moat decay vs. one-quarter noise.

        Moat scores are integer-rounded analyst-style grades, so a single
        reading one notch below a six-quarter high is ordinary jitter, not a
        broken franchise. We therefore compare the trailing max against the
        *better* of the two most recent readings: the flag only fires once
        the depressed level has held for two consecutive quarters, which a
        genuine erosion does and a noise blip does not. Costs one quarter of
        detection latency; removes nearly all false positives.
        """
        th = self.cfg.thresholds
        hist = self.store.get_history(ticker)[-th.moat_erosion_lookback:]
        # hist already includes the CURRENT quarter (pushed just before this
        # check), so hist[-1] is now, hist[-2] is last quarter, and the
        # trailing baseline is everything before those two.
        if len(hist) < 4:
            return None
        prev_moat = hist[-2][1]
        recent_best = max(current_moat, prev_moat)
        trailing_max = max(m for _, m, _ in hist[:-2])
        if trailing_max - recent_best >= th.moat_erosion_points:
            return (f"moat {trailing_max:.0f} -> {current_moat:.0f}, held "
                    f"2 qtrs (-{trailing_max - recent_best:.0f} pts vs "
                    f"trailing max)")
        return None

    def _evaluate(self, raw: FundamentalRecord) -> FilterVerdict:
        rec, anomalies = self._validate(raw)
        if rec is None:
            return FilterVerdict(
                ticker=raw.ticker, quarter=raw.quarter, passed=False,
                composite_score=0.0, anomalies=anomalies,
                rejection_reasons=["unusable data"])
        th = self.cfg.thresholds
        net_debt_ebitda = ((rec.total_debt - rec.cash) / rec.ebitda
                           if rec.ebitda and rec.ebitda > 0 else float("inf"))
        if rec.net_income and rec.net_income > 0:
            conv_gap = abs(rec.fcf / rec.net_income - 1.0)
            conv_detail = f"|FCF/NI-1|={conv_gap:.2f}"
        else:
            conv_gap = float("inf")
            conv_detail = "NI<=0, conversion unassessable"
        pio_score, pio_detail = self._piotroski_lite(rec)
        fcf_yield = (rec.fcf / rec.market_cap
                     if rec.market_cap and rec.market_cap > 0 else 0.0)
        lo, hi = th.fcf_yield_band

        layers = [
            LayerResult("L1_roic", rec.roic >= th.min_roic,
                        f"ROIC={rec.roic:.1%} (min {th.min_roic:.0%})"),
            LayerResult("L2_fcf_conversion",
                        conv_gap < th.max_fcf_conversion_gap, conv_detail),
            LayerResult("L3_piotroski", pio_score >= th.min_piotroski,
                        pio_detail),
            LayerResult("L4_net_debt_ebitda",
                        net_debt_ebitda < th.max_net_debt_ebitda,
                        f"ND/EBITDA={net_debt_ebitda:.2f}x"),
            LayerResult("L5_moat", rec.moat_score >= th.min_moat_score,
                        f"moat={rec.moat_score:.0f}/8"),
            LayerResult("VG_fcf_yield", lo <= fcf_yield <= hi,
                        f"FCF yield={fcf_yield:.1%} (band {lo:.0%}-{hi:.0%})"),
        ]
        reasons = [f"{l.name}: {l.detail}" for l in layers if not l.passed]

        # Structural moat-change detection (history lives in the store).
        self.store.push_history(rec.ticker, rec.quarter,
                                rec.moat_score, rec.roic)
        erosion = self._moat_erosion(rec.ticker, rec.moat_score)
        if erosion:
            anomalies.append(AnomalyKind.MOAT_EROSION.value)
            reasons.append(f"STRUCTURAL: {erosion}")
            self.store.incr("moat_erosion_flags")
            self.log.warning("q%-2d STRUCTURAL MOAT EROSION %s: %s",
                             rec.quarter, rec.ticker, erosion)

        passed = all(l.passed for l in layers) and erosion is None
        leverage_component = (
            min(1.0, max(0.0, (th.max_net_debt_ebitda - net_debt_ebitda)
                         / th.max_net_debt_ebitda))
            if math.isfinite(net_debt_ebitda) else 0.0)
        composite = (0.35 * min(1.0, max(0.0, rec.roic / 0.30))
                     + 0.25 * rec.moat_score / 8.0
                     + 0.20 * pio_score / 9.0
                     + 0.20 * leverage_component)
        return FilterVerdict(
            ticker=rec.ticker, quarter=rec.quarter, passed=passed,
            composite_score=round(composite, 4), layers=layers,
            anomalies=anomalies, rejection_reasons=reasons)
