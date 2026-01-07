import pandas as pd, os
from datetime import date
csv='WeeklySignalScanner-main/outputs/results/全銘柄_MA52_陽線包み_2026-01-06.csv'
if not os.path.exists(csv):
    print('CSV not found:', csv)
    raise SystemExit(1)
df=pd.read_csv(csv)
results=[]
for t in df['ticker'].astype(str):
    path=os.path.join('data', t+'.parquet')
    if not os.path.exists(path):
        results.append((t,'missing_file',None))
        continue
    try:
        dfr=pd.read_parquet(path, columns=['Close'])
        if dfr is None or dfr.empty:
            results.append((t,'no_data',None))
            continue
        last = dfr.index.max()
        d = last.date() if hasattr(last,'date') else None
        results.append((t,'has_data',str(d)))
    except Exception as e:
        results.append((t,'error',str(e)))

have = [r for r in results if r[1]=='has_data' and r[2]=='2026-01-06']
not_have = [r for r in results if not (r[1]=='has_data' and r[2]=='2026-01-06')]
print('total_csv_tickers:', len(results))
print('count_with_2026-01-06:', len(have))
print('count_without_2026-01-06:', len(not_have))
print('\nnot up-to-date or missing:')
for r in not_have:
    print(r)
