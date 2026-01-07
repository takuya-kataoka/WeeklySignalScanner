# Streamlit Cloud Deployment Guide

This guide describes how to deploy this repository to Streamlit Cloud.

1) Push repository to GitHub
   - Ensure the repository (and branch) you want to deploy is pushed to GitHub.

2) Main entrypoint
   - Streamlit Cloud expects a repository-root runnable file. This project includes `streamlit_app.py` at the repository root which runs `WeeklySignalScanner-main/app_streamlit.py`.

3) Dependencies
   - Streamlit Cloud installs Python packages from the `requirements.txt` at the repository root. The top-level `requirements.txt` includes `streamlit`, `yfinance`, `pandas`, `pyarrow`, `plotly`, etc.

4) Data directory and persistence
   - By default the app reads/writes `data/` in the repository runtime directory. Streamlit Cloud storage is ephemeral — files may be lost on redeploy.
   - Recommended: use an external object store (S3/GCS) for Parquet cache. Alternatives:
     - Set environment variable `DATA_DIR` in Streamlit Cloud to a mounted path if available.
     - Run the full fetch on a separate server/CI and upload Parquet to an object store; the app can then read from that store.

5) Streamlit Cloud app settings
   - Main file path: `streamlit_app.py`
   - Requirements file: repository root `requirements.txt` (no action required if present)
   - Environment variables: set `DATA_DIR` if you need to override the default data location

6) Post-deploy actions
   - Open the app URL provided by Streamlit Cloud.
   - Use the sidebar "管理" panel to run data downloads and scans. For large-scale full cache operations prefer doing that outside Streamlit Cloud and supply the cached files to the app.

7) Optional helper
   - I can add an S3 sync helper (upload/download Parquet) and modify the app to read from S3 if you want persistent remote caching. Ask and I will implement it.

---
If you'd like, I will also update the main `README.md` to reference this guide and add an example of setting the `DATA_DIR` environment variable in Streamlit Cloud.
