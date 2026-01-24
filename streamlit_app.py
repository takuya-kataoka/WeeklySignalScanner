"""Root wrapper for Streamlit Cloud: prepare module paths then run the app.

This ensures the nested `WeeklySignalScanner-main` folder is on `sys.path`
and attempts to load local modules (like `data_fetcher`) before running
the main app to avoid ModuleNotFoundError on cloud runtimes.
"""
import runpy
import sys
import importlib.util
from pathlib import Path

root = Path(__file__).resolve().parent
app_dir = root / 'WeeklySignalScanner-main'

# Ensure the nested app directory (and its parent) are on sys.path
for p in (str(app_dir), str(root)):
	if p and p not in sys.path:
		sys.path.insert(0, p)

# Try to load `data_fetcher.py` early as a fallback if present
try:
	mf = app_dir / 'data_fetcher.py'
	if mf.exists():
		spec = importlib.util.spec_from_file_location('data_fetcher', str(mf))
		if spec and spec.loader:
			module = importlib.util.module_from_spec(spec)
			spec.loader.exec_module(module)
			sys.modules['data_fetcher'] = module
except Exception:
	# best-effort: if this fails, the nested app will try its own fallbacks
	pass

app_path = app_dir / 'app_streamlit.py'
runpy.run_path(str(app_path), run_name='__main__')
