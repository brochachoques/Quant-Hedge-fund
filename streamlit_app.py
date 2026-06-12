
import os
import sys

# Tell Python to look inside your repository root folder
sys.path.append(os.path.dirname(__file__))

# Safely import your main file through the package structure
import equity_pipeline.main as app

# If Claude put the engine inside a main() function, run it!
if hasattr(app, "main"):
    app.main()
