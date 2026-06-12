"""Entrypoint: configure logging, run the supervisor, print the final report.

Usage:
    python -m equity_pipeline.main
    python -m equity_pipeline.main --quarters 16 --universe 30 --seed 7
    python -m equity_pipeline.main --hard-fail-rate 0.35 --hang-rate 0.15
    python -m equity_pipeline.main --weighting score --log-level DEBUG
"""
from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional, Sequence

from .config import PipelineConfig
from .supervisor import RunResult, Supervisor, render_final_report


def _setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)-22s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    # The asyncio logger gets chatty at DEBUG; keep it one notch quieter.
    logging.getLogger("asyncio").setLevel(max(level, logging.INFO))


async def run_pipeline(cfg: PipelineConfig) -> RunResult:
    """Programmatic entrypoint (also used by the stress-test suite)."""
    supervisor = Supervisor(cfg)
    return await supervisor.run()


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = PipelineConfig.from_args(list(argv) if argv is not None else None)
    _setup_logging(cfg.log_level)
    log = logging.getLogger("pipeline.main")
    try:
        result = asyncio.run(run_pipeline(cfg))
    except KeyboardInterrupt:
        log.warning("interrupted by user — partial results discarded")
        return 130
    print(render_final_report(result))
    if result.quarters_completed < result.quarters_requested:
        log.error("completed %d/%d quarters — see counters above",
                  result.quarters_completed, result.quarters_requested)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
