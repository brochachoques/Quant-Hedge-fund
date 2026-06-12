import os
import sys

# 1. Point Python to your root directory
root_dir = os.path.dirname(__file__)
sys.path.append(root_dir)

# 2. Locate your main execution file
main_path = os.path.join(root_dir, "equity_pipeline", "main.py")
with open(main_path, "r") as f:
    code = f.read()

# 3. Force Python to run the file as the primary script with package awareness
globals_dict = {
    "__name__": "__main__",
    "__file__": main_path,
    "__package__": "equity_pipeline"
}

exec(code, globals_dict)
