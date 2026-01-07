#!/usr/bin/env python3
"""
Sort results CSV by current_price column and write a _sorted file.
Usage:
  python scripts/sort_results_by_price.py path/to/file.csv [--asc]

If input has header like 'ticker,current_price' it will sort by current_price.
Output: same directory, filename with _sorted appended before .csv
"""
import argparse
import os
import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument('input', help='path to csv file')
    p.add_argument('--asc', action='store_true', help='sort ascending (default descending)')
    args = p.parse_args()

    infile = args.input
    if not os.path.exists(infile):
        print('File not found:', infile)
        raise SystemExit(1)

    df = pd.read_csv(infile)
    # try common column names
    cand = None
    for name in ['current_price', 'price', 'close', 'current']:
        if name in df.columns:
            cand = name
            break
    if cand is None:
        # try to find numeric column
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if numeric_cols:
            cand = numeric_cols[0]
        else:
            print('No numeric column found to sort by. Columns:', df.columns.tolist())
            raise SystemExit(2)

    ascending = args.asc
    df_sorted = df.sort_values(by=cand, ascending=ascending).reset_index(drop=True)

    base, ext = os.path.splitext(infile)
    out = f"{base}_sorted{ext}"
    df_sorted.to_csv(out, index=False)
    print('Wrote sorted file:', out)


if __name__ == '__main__':
    main()
