import os
import runpy
import sys


path, script, *arguments = sys.argv[1:]
python_path = [path]
if os.environ.get("PYTHONPATH"):
    python_path.append(os.environ["PYTHONPATH"])
os.environ["PYTHONPATH"] = os.pathsep.join(python_path)
sys.path.insert(0, path)
sys.argv = [script, *arguments]
runpy.run_path(script, run_name="__main__")
