import os
import glob
import pandas as pd

streamlit_path = os.path.abspath('WeeklySignalScanner-main/streamlit_netkeiba_predict.py')
print('streamlit_path=', streamlit_path)

PRED_DIR = 'outputs/predictions_by_date'
if not os.path.isdir(PRED_DIR):
    base = os.path.dirname(streamlit_path)
    alt = os.path.normpath(os.path.join(base, '..', 'outputs', 'predictions_by_date'))
    if os.path.isdir(alt):
        PRED_DIR = alt
if not os.path.isdir(PRED_DIR):
    alt2 = os.path.join(os.getcwd(), 'outputs', 'predictions_by_date')
    if os.path.isdir(alt2):
        PRED_DIR = alt2

print('Resolved PRED_DIR =', PRED_DIR)
if not os.path.isdir(PRED_DIR):
    print('PRED_DIR does not exist')
else:
    files = sorted(glob.glob(os.path.join(PRED_DIR, '*')))
    print('Total files in PRED_DIR =', len(files))
    # show index.csv info if present
    idx = os.path.join(PRED_DIR, 'index.csv')
    if os.path.exists(idx):
        try:
            df_idx = pd.read_csv(idx)
            print('index.csv columns:', list(df_idx.columns))
            if 'race_date' in df_idx.columns:
                dates = sorted(df_idx['race_date'].astype(str).unique())
                print('Dates in index.csv sample (first 10):', dates[:10])
            else:
                print('index.csv has no race_date column')
        except Exception as e:
            print('Failed to read index.csv:', e)
    else:
        # fallback: list parquet filenames
        parquets = sorted(glob.glob(os.path.join(PRED_DIR, 'predictions_*.parquet')))
        print('Found parquet files (first 10):', [os.path.basename(p) for p in parquets[:10]])

# Also emulate available_dates logic from streamlit
available_dates = []
if os.path.isdir(PRED_DIR):
    idx_path = os.path.join(PRED_DIR, 'index.csv')
    if os.path.exists(idx_path):
        try:
            df_idx = pd.read_csv(idx_path)
            if 'race_date' in df_idx.columns:
                available_dates = sorted(df_idx['race_date'].astype(str).tolist())
        except Exception:
            available_dates = []
    if not available_dates:
        parts = sorted(glob.glob(os.path.join(PRED_DIR, 'predictions_*.parquet')))
        available_dates = [os.path.basename(p).replace('predictions_','').replace('.parquet','') for p in parts]

print('available_dates sample (first 20):', available_dates[:20])
