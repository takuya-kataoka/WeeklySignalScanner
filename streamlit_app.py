"""
Root wrapper for Streamlit Cloud: ensure the nested app folder is on
`sys.path` so `import data_fetcher` and similar local imports succeed.
This is deliberately defensive to handle Streamlit Cloud working-dir
differences without changing the nested app sources.
"""
from pathlib import Path
import runpy
import sys
import importlib.util

BASE = Path(__file__).resolve().parent
APP_DIR = BASE / 'WeeklySignalScanner-main'

# Prefer the app dir first so imports like `import data_fetcher` resolve.
if str(APP_DIR) not in sys.path:
	sys.path.insert(0, str(APP_DIR))

# Also ensure repo root is available for any other top-level imports.
if str(BASE) not in sys.path:
	sys.path.insert(0, str(BASE))

# Try to ensure `data_fetcher` is available (defensive — load from file if needed).
try:
	import data_fetcher  # type: ignore
except Exception:
	df_file = APP_DIR / 'data_fetcher.py'
	if df_file.exists():
		spec = importlib.util.spec_from_file_location('data_fetcher', str(df_file))
		if spec and spec.loader:
			module = importlib.util.module_from_spec(spec)
			spec.loader.exec_module(module)  # type: ignore
			sys.modules['data_fetcher'] = module

app_path = APP_DIR / 'app_streamlit.py'
runpy.run_path(str(app_path), run_name='__main__')
