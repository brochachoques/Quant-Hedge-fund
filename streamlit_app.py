import os
import sys
import runpy

# Add the root folder to Python's search path
sys.path.insert(0, os.path.dirname(__file__))

# Run your main file as '__main__' while preserving its package context
runpy.run_module("equity_pipeline.main", run_name="__main__")
