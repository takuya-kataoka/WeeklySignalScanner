# Root wrapper for Streamlit Cloud: run the existing app_streamlit.py
import runpy
from pathlib import Path

app_path = Path(__file__).resolve().parent / 'WeeklySignalScanner-main' / 'app_streamlit.py'
runpy.run_path(str(app_path), run_name='__main__')
