# Root wrapper for Streamlit Cloud: run the existing app_streamlit.py
import runpy
from pathlib import Path

#!/usr/bin/env python3
# Root wrapper for Streamlit Cloud: run the existing app_streamlit.py
import runpy
import sys
from pathlib import Path

# Ensure the app directory is on sys.path so imports inside app_streamlit.py work
app_path = Path(__file__).resolve().parent / 'WeeklySignalScanner-main' / 'app_streamlit.py'
app_dir = app_path.parent
if str(app_dir) not in sys.path:
	sys.path.insert(0, str(app_dir))

runpy.run_path(str(app_path), run_name='__main__')
