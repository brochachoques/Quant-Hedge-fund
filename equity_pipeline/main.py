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

import streamlit as st  # 🛠️ Added Streamlit support

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
    logging.getLogger("asyncio").setLevel(max(level, logging.INFO))


async def run_pipeline(cfg: PipelineConfig) -> RunResult:
    """Programmatic entrypoint (also used by the stress-test suite)."""
    supervisor = Supervisor(cfg)
    return await supervisor.run()


def main(argv: Optional[Sequence[str]] = None) -> int:
    # 🛠️ STREAMLIT UI LAYOUT
    st.title("📊 Multi-Agent Equity Pipeline")
    st.subheader("Quantitative Simulation Dashboard")

    # If running inside Streamlit, ignore command-line flags to prevent server lockups
    if argv is None and any("streamlit" in arg for arg in sys.argv):
        cfg = PipelineConfig.from_args([])  # Fall back safely to pipeline defaults
    else:
        cfg = PipelineConfig.from_args(list(argv) if argv is not None else None)

    _setup_logging(cfg.log_level)
    log = logging.getLogger("pipeline.main")

    st.info("The quantitative supervisor engine is idling safely and ready for execution.")

    # 🛠️ Wrap execution in a UI button to prevent startup hangs
    if st.button("🚀 Run Multi-Agent Pipeline", type="primary"):
        with st.spinner("Supervisor assembling agents and running quarters..."):
            try:
                result = asyncio.run(run_pipeline(cfg))
                
                st.success("Simulation complete!")
                
                # Render the final report directly onto the web interface
                st.markdown("### 📋 Final Simulation Report")
                st.code(render_final_report(result), language="text")

                if result.quarters_completed < result.quarters_requested:
                    st.error(f"Warning: Only completed {result.quarters_completed}/{result.quarters_requested} quarters. Check terminal logs.")
                    return 1
                return 0

            except KeyboardInterrupt:
                log.warning("interrupted by user — partial results discarded")
                st.warning("Execution interrupted.")
                return 130
            except Exception as e:
                st.error(f"Pipeline encountered a critical error: {e}")
                log.exception("Pipeline crash caught in UI loop.")
                return 1
    
    return 0


if __name__ == "__main__":
    main()
