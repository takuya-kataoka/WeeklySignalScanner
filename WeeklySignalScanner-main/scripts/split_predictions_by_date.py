"""Split predictions file by race_date into per-day parquet files.

Usage:
  python3 scripts/split_predictions_by_date.py 
Environment variables:
  PRED_IN  - input parquet (default: outputs/netkeiba_predictions_with_probs.parquet)
  OUT_DIR  - output dir (default: outputs/predictions_by_date)
"""
import os
import pandas as pd
import pathlib

PRED_IN = os.environ.get('PRED_IN', 'outputs/netkeiba_predictions_with_probs.parquet')
OUT_DIR = os.environ.get('OUT_DIR', 'outputs/predictions_by_date')


def main():
    if not os.path.exists(PRED_IN):
        print('Input not found:', PRED_IN)
        return
    df = pd.read_parquet(PRED_IN)
    os.makedirs(OUT_DIR, exist_ok=True)
    # ensure race_date exists
    if 'race_date' not in df.columns:
        print('No race_date column in input; writing single file')
        outp = os.path.join(OUT_DIR, 'predictions_unknown_date.parquet')
        df.to_parquet(outp, index=False)
        print('Wrote', outp, 'rows=', len(df))
        return
    counts = {}
    for date, g in df.groupby('race_date'):
        safe_date = str(date).replace(':','_')
        outp = os.path.join(OUT_DIR, f'predictions_{safe_date}.parquet')
        g.to_parquet(outp, index=False)
        counts[safe_date] = len(g)
    # write a simple index
    idx = pd.DataFrame([{'race_date':k,'rows':v} for k,v in sorted(counts.items())])
    idx.to_csv(os.path.join(OUT_DIR, 'index.csv'), index=False)
    print('Wrote', len(counts), 'files into', OUT_DIR)
    for k,v in list(counts.items())[:10]:
        print(k, v)


if __name__ == '__main__':
    main()
