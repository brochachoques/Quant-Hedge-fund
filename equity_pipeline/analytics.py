"""Performance analytics over equity curves. Pure functions, stdlib only."""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import List


def daily_returns(curve: List[float]) -> List[float]:
    return [curve[i] / curve[i - 1] - 1.0
            for i in range(1, len(curve))
            if curve[i - 1] > 0]


def cagr(curve: List[float], periods_per_year: int = 252) -> float:
    if len(curve) < 2 or curve[0] <= 0 or curve[-1] <= 0:
        return 0.0
    years = (len(curve) - 1) / periods_per_year
    if years <= 0:
        return 0.0
    return (curve[-1] / curve[0]) ** (1.0 / years) - 1.0


def annualized_vol(curve: List[float], periods_per_year: int = 252) -> float:
    rets = daily_returns(curve)
    if len(rets) < 2:
        return 0.0
    return statistics.pstdev(rets) * math.sqrt(periods_per_year)


def sharpe(curve: List[float], risk_free: float = 0.0,
           periods_per_year: int = 252) -> float:
    vol = annualized_vol(curve, periods_per_year)
    if vol == 0:
        return 0.0
    return (cagr(curve, periods_per_year) - risk_free) / vol


def max_drawdown(curve: List[float]) -> float:
    peak, mdd = float("-inf"), 0.0
    for v in curve:
        peak = max(peak, v)
        if peak > 0:
            mdd = max(mdd, 1.0 - v / peak)
    return mdd


@dataclass
class PerformanceSummary:
    total_return: float
    cagr: float
    ann_vol: float
    sharpe: float
    max_drawdown: float
    bench_total_return: float
    bench_cagr: float
    bench_max_drawdown: float
    avg_quarterly_turnover: float
    total_costs: float
    quarters: int

    def as_lines(self) -> List[str]:
        f = lambda x: f"{x * 100:7.2f}%"
        return [
            f"  Strategy total return   {f(self.total_return)}",
            f"  Strategy CAGR           {f(self.cagr)}",
            f"  Strategy ann. vol       {f(self.ann_vol)}",
            f"  Strategy Sharpe         {self.sharpe:8.2f}",
            f"  Strategy max drawdown   {f(self.max_drawdown)}",
            f"  Benchmark total return  {f(self.bench_total_return)}",
            f"  Benchmark CAGR          {f(self.bench_cagr)}",
            f"  Benchmark max drawdown  {f(self.bench_max_drawdown)}",
            f"  Avg quarterly turnover  {f(self.avg_quarterly_turnover)}",
            f"  Cumulative txn costs    {self.total_costs:8.5f} (of 1.0 start NAV)",
            f"  Quarters completed      {self.quarters:8d}",
        ]


def summarize(curve: List[float], bench: List[float],
              turnovers: List[float], total_costs: float,
              risk_free: float = 0.0) -> PerformanceSummary:
    return PerformanceSummary(
        total_return=(curve[-1] / curve[0] - 1.0) if len(curve) > 1 else 0.0,
        cagr=cagr(curve),
        ann_vol=annualized_vol(curve),
        sharpe=sharpe(curve, risk_free),
        max_drawdown=max_drawdown(curve),
        bench_total_return=(bench[-1] / bench[0] - 1.0) if len(bench) > 1 else 0.0,
        bench_cagr=cagr(bench),
        bench_max_drawdown=max_drawdown(bench),
        avg_quarterly_turnover=(sum(turnovers) / len(turnovers)) if turnovers else 0.0,
        total_costs=total_costs,
        quarters=len(turnovers),
    )
