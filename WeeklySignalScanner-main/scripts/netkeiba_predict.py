"""
Simple predictor for enriched netkeiba data.
- Loads an enriched parquet file
- Trains a logistic regression to predict win (rank==1)
- Predicts win_prob for current target rows
- Generates betting suggestions with bankroll per race fixed
"""
import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LogisticRegression

IN_PATH = os.environ.get('NETKEIBA_IN', 'outputs/netkeiba_enriched_test.parquet')
OUT_PATH = os.environ.get('NETKEIBA_OUT', 'outputs/netkeiba_predictions.parquet')
BANKROLL_PER_RACE = float(os.environ.get('BANKROLL_PER_RACE', '1000'))
MIN_EV = float(os.environ.get('MIN_EV', '1.10'))
MAX_BETS_PER_RACE = int(os.environ.get('MAX_BETS_PER_RACE', '3'))


def featurize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # odds
    df['odds_f'] = pd.to_numeric(df['odds'], errors='coerce').fillna(100.0)
    # recent mean rank
    def mean_rank(x):
        try:
            if x is None:
                return np.nan
            # numpy arrays / lists
            import numpy as _np, ast
            if isinstance(x, (_np.ndarray, list, tuple)):
                try:
                    vals = [float(i) for i in x]
                    return _np.mean(vals) if vals else _np.nan
                except Exception:
                    return _np.nan
            if isinstance(x, str):
                # attempt to eval list-like
                try:
                    v = ast.literal_eval(x)
                    if isinstance(v, (list, tuple)):
                        return _np.mean([float(i) for i in v]) if v else _np.nan
                except Exception:
                    # fallback: find digits
                    m = re.findall(r'\d+', x)
                    if m:
                        return _np.mean([float(i) for i in m])
                    return _np.nan
            return _np.nan
        except Exception:
            return np.nan
    # handle missing recent_ranks column
    if 'recent_ranks' in df.columns:
        df['recent_mean_rank'] = df['recent_ranks'].apply(mean_rank)
    else:
        df['recent_mean_rank'] = np.nan
    # age
    def age_from_age_sex(x):
        if not x or pd.isna(x):
            return np.nan
        m = None
        import re
        m = re.search(r'(\d{1,2})', str(x))
        if m:
            return float(m.group(1))
        return np.nan
    # handle missing age_sex
    if 'age_sex' in df.columns:
        df['age'] = df['age_sex'].apply(age_from_age_sex)
    else:
        df['age'] = np.nan
    # lineage group one-hot (small)
    if 'father_group' in df.columns:
        df['father_group'] = df['father_group'].fillna('不明')
    else:
        df['father_group'] = '不明'
    df = pd.concat([df, pd.get_dummies(df['father_group'], prefix='lg')], axis=1)
    # fillna
    df['recent_mean_rank'] = df['recent_mean_rank'].fillna(df['recent_mean_rank'].median())
    df['age'] = df['age'].fillna(df['age'].median())
    return df


def train_and_predict(df_all: pd.DataFrame, df_target: pd.DataFrame):
    # select rows for training where rank is available
    if 'rank' in df_all.columns:
        df_train = df_all[df_all['rank'].notna()].copy()
    else:
        df_train = pd.DataFrame()
    if df_train is None or len(df_train) < 20:
        # fallback: use target as training if insufficient history or missing ranks
        df_train = df_target.copy()
    # create is_win safely even if 'rank' missing
    if 'rank' in df_train.columns:
        df_train['is_win'] = (df_train['rank'] == 1).astype(int)
    else:
        df_train['is_win'] = 0
    # featurize
    df_train = featurize(df_train)
    df_target = featurize(df_target)
    feature_cols = ['odds_f', 'recent_mean_rank', 'age'] + [c for c in df_train.columns if c.startswith('lg_')]
    # ensure cols
    feature_cols = [c for c in feature_cols if c in df_train.columns]
    X = df_train[feature_cols].fillna(0)
    y = df_train['is_win']
    if len(df_train) >= 5 and len(y.unique()) > 1:
        model = LogisticRegression(max_iter=200)
        model.fit(X, y)
        # predict for target
        Xt = df_target[feature_cols].fillna(0)
        probs = model.predict_proba(Xt)[:,1]
    else:
        # fallback simple heuristic: inverse odds normalized
        Xt = df_target[feature_cols].fillna(0)
        probs = 1.0 / df_target['odds_f']
        probs = probs / probs.max()
    df_target['win_prob'] = probs
    df_target['expected_value'] = df_target['win_prob'] * df_target['odds_f']
    # per race, select up to MAX_BETS_PER_RACE with expected_value >= MIN_EV
    out_rows = []
    for race, g in df_target.groupby('race_url'):
        cand = g.sort_values('expected_value', ascending=False)
        sel = cand[cand['expected_value'] >= MIN_EV].head(MAX_BETS_PER_RACE)
        if len(sel) == 0:
            continue
        # allocate bankroll equally among selected bets
        stake_each = int(round(BANKROLL_PER_RACE / len(sel)))
        for _, row in sel.iterrows():
            out_rows.append({
                'race_url': race,
                'race_date': row['race_date'],
                'horse_name': row['horse_name'],
                'odds': row['odds_f'],
                'win_prob': float(row['win_prob']),
                'expected_value': float(row['expected_value']),
                'stake': stake_each,
            })
    out_df = pd.DataFrame(out_rows)
    return df_target, out_df


def main():
    if not os.path.exists(IN_PATH):
        print('Input not found:', IN_PATH)
        return
    df_all = None
    hist_path = 'outputs/netkeiba_2023_2026.parquet'
    if os.path.exists(hist_path):
        try:
            df_all = pd.read_parquet(hist_path)
        except Exception:
            df_all = None
    df_target = pd.read_parquet(IN_PATH)
    # Only predict for 未確定レース: rank が NaN (まだ着順が入っていないもの)
    if 'rank' in df_target.columns:
        df_target_unseen = df_target[df_target['rank'].isna()].copy()
        if df_target_unseen.empty:
            print('No unseen (unresolved) races in input; using all rows as target for fallback.')
            df_target_unseen = df_target.copy()
    else:
        # missing 'rank' column (e.g., freshly scraped shutuba) -> treat all rows as unseen
        df_target_unseen = df_target.copy()
    # ensure race_url exists for grouping; synthesize if missing
    if 'race_url' not in df_target_unseen.columns:
        df_target_unseen['race_url'] = 'jra_parsed_' + os.path.basename(IN_PATH)
    if 'race_date' not in df_target_unseen.columns:
        df_target_unseen['race_date'] = os.path.basename(IN_PATH)
    if df_all is None:
        df_all = df_target.copy()
    df_pred, bets = train_and_predict(df_all, df_target_unseen)
    df_pred.to_parquet(OUT_PATH.replace('.parquet','_with_probs.parquet'), index=False)
    bets.to_parquet(OUT_PATH, index=False)
    # if no bets, output top-1 predicted horse per race for reference
    if bets.empty:
        top1_rows = []
        for race, g in df_pred.groupby('race_url'):
            top = g.sort_values('win_prob', ascending=False).head(1).iloc[0]
            top1_rows.append({
                'race_url': race,
                'race_date': top['race_date'],
                'horse_name': top['horse_name'],
                'odds': float(top['odds_f']),
                'win_prob': float(top['win_prob']),
                'expected_value': float(top['expected_value']),
                'stake': 0,
                'note': 'top1_fallback'
            })
        top1_df = pd.DataFrame(top1_rows)
        top1_path = OUT_PATH.replace('.parquet','_top1.parquet')
        top1_df.to_parquet(top1_path, index=False)
        print('No EV bets; saved top-1 predictions to', top1_path)
    else:
        print('Saved predictions to', OUT_PATH)
        print('Bet suggestions:')
        print(bets.to_string())

if __name__ == '__main__':
    main()
