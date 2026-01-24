"""Wrapper to run the `WeeklySignalScanner-main/app_predict.py` under Streamlit Cloud.

Place this file as the app entrypoint in Streamlit Cloud (set "Main file path" to
`streamlit_predict_app.py`). It adjusts `sys.path` so the nested package can be
imported and attempts to preload local modules like `data_fetcher`.
"""
import runpy
import sys
import importlib.util
from pathlib import Path

root = Path(__file__).resolve().parent
app_dir = root / 'WeeklySignalScanner-main'

# Ensure nested app directory and repo root are on sys.path
for p in (str(app_dir), str(root)):
    if p and p not in sys.path:
        sys.path.insert(0, p)

# Best-effort preload of common local modules
for mod in ('data_fetcher', 'data_fetcher_us'):
    try:
        mf = app_dir / f"{mod}.py"
        if mf.exists():
            spec = importlib.util.spec_from_file_location(mod, str(mf))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                sys.modules[mod] = module
    except Exception:
        pass

# Run the predict app
app_path = app_dir / 'app_predict.py'
runpy.run_path(str(app_path), run_name='__main__')
