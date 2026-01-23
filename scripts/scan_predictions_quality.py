import os, glob, pandas as pd
PRED_DIR = os.path.join('outputs','predictions_by_date')
if not os.path.isdir(PRED_DIR):
    print('pred dir missing', PRED_DIR); raise SystemExit(1)
parqs = sorted(glob.glob(os.path.join(PRED_DIR,'predictions_*.parquet')))
print('found', len(parqs), 'parquet files')
problems = []
for p in parqs:
    date = os.path.basename(p).replace('predictions_','').replace('.parquet','')
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        problems.append((date, 'read_error', str(e)))
        continue
    n = len(df)
    has_horse = df['horse_name'].notna().sum() if 'horse_name' in df.columns else 0
    has_race = df['race_url'].astype(str).str.contains('/race/').sum() if 'race_url' in df.columns else 0
    odds_num = 0
    if 'odds_f' in df.columns:
        odds_num = pd.to_numeric(df['odds_f'], errors='coerce').notna().sum()
    elif 'odds' in df.columns:
        odds_num = pd.to_numeric(df['odds'], errors='coerce').notna().sum()
    problems.append((date, n, has_horse, has_race, odds_num))

# print summary of problematic dates
bad = [r for r in problems if isinstance(r[1], int) and (r[2]==0 or r[3]==0 or r[4]==0)]
print('problematic count:', len(bad))
for row in bad[:200]:
    date, n, has_horse, has_race, odds_num = row
    print(date, 'rows=', n, 'horse_name=', has_horse, '/race/=', has_race, 'odds_num=', odds_num)

# write CSV
out='outputs/predictions_quality_report.csv'
df_out = pd.DataFrame([{
    'date':r[0], 'rows':r[1] if isinstance(r[1], int) else None,
    'horse_name_count':r[2] if isinstance(r[1], int) else None,
    'race_url_race_count':r[3] if isinstance(r[1], int) else None,
    'odds_numeric_count':r[4] if isinstance(r[1], int) else None
} for r in problems])
df_out.to_csv(out, index=False)
print('wrote', out)
