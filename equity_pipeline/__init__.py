"""Multi-agent, supervised, fault-tolerant equity data pipeline.

Four concurrent agents (Ingestion, AnalyticalFilter, BacktestExecution,
SelfCorrection) coordinated by a Supervisor over asyncio queues, with
externalized state in a DataStore so any agent can be killed and
restarted mid-run without losing the pipeline.
"""

__version__ = "1.0.0"
