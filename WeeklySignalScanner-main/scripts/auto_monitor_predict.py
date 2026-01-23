"""Auto monitor enriched parquet, run predict, split by date, validate and restart Streamlit.

Usage: set env vars as needed and run this script in background.
ENV:
  WATCH_PATH - path to enriched parquet (default outputs/netkeiba_2023_2026_enriched.parquet)
  INTERVAL - check interval seconds (default 600)
  MIN_VALID_ROWS - minimum rows with valid horse_name and odds to consider a day valid (default 5)
  STREAMLIT_CMD - command to start streamlit (default uses WeeklySignalScanner-main/streamlit_netkeiba_predict.py on port 8504)
"""
import os
import time
import subprocess
from datetime import datetime
import pandas as pd

WATCH_PATH = os.environ.get('WATCH_PATH', 'outputs/netkeiba_2023_2026_enriched.parquet')
INTERVAL = int(os.environ.get('INTERVAL', '600'))
MIN_VALID_ROWS = int(os.environ.get('MIN_VALID_ROWS', '3'))
PREDICT_CMD = os.environ.get('PREDICT_CMD', 'PYTHONPATH=WeeklySignalScanner-main NETKEIBA_IN=outputs/netkeiba_2023_2026_enriched.parquet BANKROLL_PER_RACE=1000 python3 WeeklySignalScanner-main/scripts/netkeiba_predict.py')
SPLIT_CMD = os.environ.get('SPLIT_CMD', 'python3 WeeklySignalScanner-main/scripts/split_predictions_by_date.py')
STREAMLIT_CMD = os.environ.get('STREAMLIT_CMD', 'streamlit run WeeklySignalScanner-main/streamlit_netkeiba_predict.py --server.port 8504 --server.headless true')

LOG = os.environ.get('AUTO_MONITOR_LOG', 'outputs/auto_monitor_predict.log')


def log(msg):
    ts = datetime.utcnow().isoformat()
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass


def file_mtime(path):
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


def run_cmd(cmd):
    log('Running: ' + cmd)
    try:
        r = subprocess.run(cmd, shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for l in r.stdout.splitlines():
            log('OUT: ' + l)
        return r.returncode
    except Exception as e:
        log('Cmd failed: ' + repr(e))
        return 1


def validate_predictions_by_date(outdir='outputs/predictions_by_date'):
    """Return True if any per-day file contains at least MIN_VALID_ROWS with valid horse_name and odds_display."""
    if not os.path.isdir(outdir):
        return False
    try:
        idx = pd.read_csv(os.path.join(outdir, 'index.csv')) if os.path.exists(os.path.join(outdir, 'index.csv')) else None
    except Exception:
        idx = None
    files = []
    if idx is not None and 'race_date' in idx.columns:
        for d in idx['race_date'].astype(str).tolist():
            files.append(os.path.join(outdir, f'predictions_{d}.parquet'))
    else:
        # fallback: list files
        import glob
        files = sorted(glob.glob(os.path.join(outdir, 'predictions_*.parquet')))
    for f in files:
        try:
            df = pd.read_parquet(f)
            if 'horse_name' in df.columns and 'odds' in df.columns:
                # numeric odds
                odds = pd.to_numeric(df.get('odds') if 'odds' in df.columns else df.get('odds_f'), errors='coerce')
                valid = df['horse_name'].notna() & odds.notna() & (odds > 1) & (odds < 1000)
                cnt = int(valid.sum())
                log(f'Validated {f} rows_valid={cnt}')
                if cnt >= MIN_VALID_ROWS:
                    return True
        except Exception as e:
            log('validate read failed: ' + repr(e))
    return False


def restart_streamlit():
    # find existing streamlit process for our app and kill it
    try:
        import psutil
        for p in psutil.process_iter(['pid','cmdline']):
            cmd = ' '.join(p.info.get('cmdline') or [])
            if 'streamlit' in cmd and 'streamlit_netkeiba_predict.py' in cmd:
                try:
                    log('Killing streamlit pid=' + str(p.info['pid']))
                    p.kill()
                except Exception:
                    pass
    except Exception:
        # fallback to pgrep/kill
        run_cmd("pkill -f 'streamlit .*streamlit_netkeiba_predict.py' || true")
    # start streamlit
    run_cmd('nohup ' + STREAMLIT_CMD + " > outputs/streamlit_netkeiba_predict.log 2>&1 & echo $! > streamlit_netkeiba_predict.pid")


def main():
    log('auto_monitor_predict started, watching ' + WATCH_PATH)
    last = file_mtime(WATCH_PATH)
    # initial run: do nothing if no file
    while True:
        try:
            cur = file_mtime(WATCH_PATH)
            if cur and cur != last:
                log('Detected updated enriched parquet')
                last = cur
                # run predict
                run_cmd(PREDICT_CMD)
                # split by date
                run_cmd(SPLIT_CMD)
                # validate per-day outputs
                ok = validate_predictions_by_date()
                if ok:
                    log('Valid per-day predictions detected; restarting Streamlit')
                    restart_streamlit()
                else:
                    log('No valid per-day predictions yet')
            time.sleep(INTERVAL)
        except KeyboardInterrupt:
            log('auto_monitor_predict stopped by KeyboardInterrupt')
            break
        except Exception as e:
            log('auto_monitor_predict error: ' + repr(e))
            time.sleep(INTERVAL)


if __name__ == '__main__':
    main()
