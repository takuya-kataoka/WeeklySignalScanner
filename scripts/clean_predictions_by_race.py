#!/usr/bin/env python3
"""Clean per-race prediction parquet files that lack usable horse data.

Criteria for moving to `deleted/`:
- missing `horse_name` column OR zero non-null `horse_name` values
- OR missing `rank` column OR zero non-null `rank` values

Files are moved (not permanently deleted) to
`outputs/predictions_by_race/deleted/<original_path>`.
Summary CSV is updated at `outputs/predictions_by_race/summary_by_race.csv`.
"""
import shutil
import pathlib
import pandas as pd
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASE = ROOT / 'outputs' / 'predictions_by_race'
DELETED = BASE / 'deleted'


def should_delete(df: pd.DataFrame) -> bool:
    # If horse_name missing or all null -> delete
    if 'horse_name' not in df.columns:
        return True
    if df['horse_name'].notna().sum() == 0:
        return True
    # If rank missing or all null -> delete
    if 'rank' not in df.columns:
        return True
    if df['rank'].notna().sum() == 0:
        return True
    return False


def main():
    if not BASE.exists():
        print('No per-race directory found at', BASE)
        return
    moved = []
    for p in BASE.rglob('*.parquet'):
        # skip files in deleted folder
        if DELETED in p.parents:
            continue
        try:
            df = pd.read_parquet(p)
        except Exception as e:
            # unreadable -> move to deleted
            reason = f'unreadable: {e}'
            dest = DELETED / p.relative_to(BASE)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), str(dest))
            moved.append((str(p.relative_to(ROOT)), str(dest.relative_to(ROOT)), reason))
            continue
        if should_delete(df):
            dest = DELETED / p.relative_to(BASE)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), str(dest))
            moved.append((str(p.relative_to(ROOT)), str(dest.relative_to(ROOT)), 'no horse_name or rank'))

    if moved:
        print('Moved', len(moved), 'files to deleted/:')
        for src, dst, reason in moved:
            print('-', src, '->', dst, f'({reason})')
    else:
        print('No files needed moving.')

    # Update summary CSV if exists
    summary_path = BASE / 'summary_by_race.csv'
    if summary_path.exists():
        try:
            s = pd.read_csv(summary_path)
            # files in summary are stored relative to ROOT; remove moved ones
            moved_srcs = {src for src, _, _ in moved}
            s2 = s[~s['file'].isin(moved_srcs)]
            s2.to_csv(summary_path, index=False)
            print('Updated summary_by_race.csv (removed moved files).')
        except Exception as e:
            print('Failed to update summary CSV:', e)
    else:
        print('No summary CSV to update.')


if __name__ == '__main__':
    main()
