"""
Supervisor: watch for enriched parquet updates and run prediction pipeline automatically.
Runs netkeiba_predict.py when `outputs/netkeiba_2023_2026_enriched.parquet` is created/updated.
Default interval: 600s (10 minutes). Logs to outputs/netkeiba_supervisor.log.
"""
import os
import time
import subprocess
from datetime import datetime

CHECK_PATH = os.environ.get('NETKEIBA_ENRICHED_PATH', 'outputs/netkeiba_2023_2026_enriched.parquet')
PREDICT_CMD = os.environ.get('NETKEIBA_PREDICT_CMD', 'PYTHONPATH=WeeklySignalScanner-main BANKROLL_PER_RACE=1000 python3 WeeklySignalScanner-main/scripts/netkeiba_predict.py')
LOG_PATH = os.environ.get('NETKEIBA_SUPERVISOR_LOG', 'outputs/netkeiba_supervisor.log')
INTERVAL = int(os.environ.get('NETKEIBA_SUPERVISOR_INTERVAL', '600'))


def log(msg):
    ts = datetime.utcnow().isoformat()
    line = f"[{ts}] {msg}\n"
    with open(LOG_PATH, 'a') as f:
        f.write(line)
    print(line, end='')


def file_mtime(path):
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


def run_predict():
    log('Triggering prediction command: ' + PREDICT_CMD)
    try:
        r = subprocess.run(PREDICT_CMD, shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        log('Predict exitcode=' + str(r.returncode))
        for l in r.stdout.splitlines():
            log('PREDICT: ' + l)
    except Exception as e:
        log('Predict failed: ' + repr(e))


def main():
    log('Supervisor started. Watching ' + CHECK_PATH + ' every ' + str(INTERVAL) + 's')
    last_mtime = file_mtime(CHECK_PATH)
    # initial run if file exists
    if last_mtime:
        log('Initial file exists; running prediction once')
        run_predict()
    while True:
        try:
            cur = file_mtime(CHECK_PATH)
            if cur and cur != last_mtime:
                log('Detected update (mtime changed)')
                last_mtime = cur
                run_predict()
            time.sleep(INTERVAL)
        except KeyboardInterrupt:
            log('Supervisor stopped by KeyboardInterrupt')
            break
        except Exception as e:
            log('Supervisor error: ' + repr(e))
            time.sleep(INTERVAL)

if __name__ == '__main__':
    main()
