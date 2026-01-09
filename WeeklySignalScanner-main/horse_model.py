"""
horse_model.py

競馬の血統処理、LightGBM学習、予想・購入金額算出（ハーフケリー）をまとめたユーティリティ。

使い方例:
    proc = HorseDataProcessor()
    df = proc.load_and_merge_results_pedigree(results_csv, pedigree_csv)
    df = proc.process_lineage_and_conditions(df, father_col='father_name')
    df, encoder = proc.fit_target_encode(df, cat_col='lineage_group', target_col='is_win')

    model, feature_names = train_lgbm_model(df, features=[...], target='is_win')
    plot_feature_importance(model, feature_names)

    preds = model.predict_proba(X)[:,1]
    bets = generate_bet_strategy(horses_df_with_preds, bankroll=50000)

"""
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import numpy as np

try:
    import matplotlib.pyplot as plt
except Exception:
    plt = None

try:
    import lightgbm as lgb
except Exception:
    lgb = None

from sklearn.model_selection import train_test_split


class HorseDataProcessor:
    """馬データの読み込みと前処理を行うユーティリティクラス。

    主な機能:
    - レース結果と5代血統（ペディグリー）CSV読み込み・結合
    - 父馬名から大系統にマッピングして `lineage_group` を追加
    - トラック状態を数値化（良=0,稍重=1,重=2,不良=3）
    - ターゲットエンコーディングの適用（平滑化あり）
    """

    DEFAULT_TRACK_MAP = {
        '良': 0,
        '稍重': 1,
        '重': 2,
        '不良': 3,
    }

    def __init__(self, lineage_map: Optional[Dict[str, str]] = None):
        # lineage_map: 父馬名の一部/完全一致 -> 系統名
        self.lineage_map = lineage_map or self._default_lineage_map()
        self._target_encoder: Dict[str, Dict[str, float]] = {}

    def _default_lineage_map(self) -> Dict[str, str]:
        # 簡易的なデフォルト辞書（実運用時は拡張推奨）
        return {
            'サンデーサイレンス': 'サンデーサイレンス系',
            'サンデー': 'サンデーサイレンス系',
            'キングマンボ': 'キングマンボ系',
            'マンボ': 'キングマンボ系',
            'ディープインパクト': 'ディープ系',
            'ディープ': 'ディープ系',
            'ハーツクライ': 'ハーツクライ系',
        }

    def load_and_merge_results_pedigree(self, results_csv: str, pedigree_csv: str,
                                        on: str = 'horse_id') -> pd.DataFrame:
        """結果と血統データを読み込みマージして返す。

        引数はCSVまたはParquetのパス／ファイルオブジェクトを受け付けます。
        両データは共通キー（デフォルト `horse_id`）でマージされます。
        """
        df_res = self._read_table(results_csv)
        df_ped = self._read_table(pedigree_csv)
        df = pd.merge(df_res, df_ped, how='left', on=on)
        return df

    def _read_table(self, source) -> pd.DataFrame:
        """汎用読み込み: CSV / Parquet に対応。

        - source が文字列パスなら拡張子で判定して読み込み
        - source がファイルオブジェクトなら name 属性やバイト内容で判定
        """
        # パス（str/Path）で指定されている場合
        try:
            if isinstance(source, (str,)) and source.lower().endswith('.parquet'):
                return pd.read_parquet(source)
            if isinstance(source, (str,)) and source.lower().endswith('.csv'):
                return pd.read_csv(source)
        except Exception:
            pass

        # ファイルオブジェクトまたはURL取得のバイトコンテンツなど
        # pandas はファイルオブジェクトから自動判定できるが、parquetはBytesIOで渡す必要がある
        try:
            # file-like: check for read() method
            if hasattr(source, 'read'):
                # try to inspect name
                name = getattr(source, 'name', '')
                source.seek(0)
                if isinstance(name, str) and name.lower().endswith('.parquet'):
                    return pd.read_parquet(source)
                # fallback: try read_csv first
                source.seek(0)
                try:
                    return pd.read_csv(source)
                except Exception:
                    source.seek(0)
                    return pd.read_parquet(source)
        except Exception:
            pass

        # 最終手段: try to let pandas infer (works for local bytes)
        try:
            return pd.read_csv(source)
        except Exception:
            return pd.read_parquet(source)

    def save_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        """DataFrame を Parquet で保存するユーティリティ。"""
        df.to_parquet(path, index=False)

    def map_lineage(self, father_name: Optional[str]) -> str:
        if pd.isna(father_name):
            return '不明'
        for key, val in self.lineage_map.items():
            if key in str(father_name):
                return val
        return 'その他'

    def process_lineage_and_conditions(self, df: pd.DataFrame,
                                       father_col: str = 'father_name',
                                       track_col: str = 'track_condition') -> pd.DataFrame:
        """系統マッピングと馬場状態の数値化を行う。"""
        df = df.copy()
        df['lineage_group'] = df[father_col].apply(self.map_lineage)
        df['track_numeric'] = df[track_col].map(self.DEFAULT_TRACK_MAP)
        df['track_numeric'] = df['track_numeric'].fillna(0).astype(int)
        return df

    def fit_target_encode(self, df: pd.DataFrame, cat_col: str, target_col: str,
                          prior_weight: float = 10.0) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """カテゴリ列にターゲットエンコーディングを学習・適用する。

        平滑化: posterior = (sum_y + prior*global_mean) / (count + prior)
        返り値: (df_transformed, encoder_dict)
        """
        df = df.copy()
        global_mean = df[target_col].mean()
        grp = df.groupby(cat_col)[target_col].agg(['sum', 'count'])
        # smoothing
        grp['smooth'] = (grp['sum'] + prior_weight * global_mean) / (grp['count'] + prior_weight)
        encoder = grp['smooth'].to_dict()
        # store encoder for later use
        self._target_encoder[cat_col] = {
            'mapping': encoder,
            'global_mean': float(global_mean),
            'prior_weight': float(prior_weight),
        }
        df[f'{cat_col}_te'] = df[cat_col].map(encoder).fillna(global_mean)
        return df, self._target_encoder[cat_col]

    def transform_with_target_encoder(self, df: pd.DataFrame, cat_col: str) -> pd.DataFrame:
        df = df.copy()
        enc = self._target_encoder.get(cat_col)
        if not enc:
            raise ValueError('encoder for column not fitted: ' + cat_col)
        df[f'{cat_col}_te'] = df[cat_col].map(enc['mapping']).fillna(enc['global_mean'])
        return df


def train_lgbm_model(df: pd.DataFrame, features: List[str], target: str,
                     test_size: float = 0.2, random_state: int = 42,
                     num_boost_round: int = 1000, early_stopping_rounds: int = 50) -> Tuple[Any, List[str]]:
    """LightGBM で二値分類モデルを学習するユーティリティ。

    - is_unbalance=True を設定
    - 評価は AUC と binary_logloss
    返り値: (model, feature_names)
    """
    if lgb is None:
        raise RuntimeError('lightgbm がインストールされていません。')

    X = df[features]
    y = df[target]
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y)

    params = {
        'objective': 'binary',
        'is_unbalance': True,
        'metric': ['auc', 'binary_logloss'],
        'verbosity': -1,
        'seed': random_state,
    }

    train_data = lgb.Dataset(X_train, label=y_train)
    valid_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

    model = lgb.train(params, train_data, num_boost_round=num_boost_round,
                      valid_sets=[valid_data], early_stopping_rounds=early_stopping_rounds,
                      verbose_eval=False)

    return model, list(features)


def plot_feature_importance(model: Any, feature_names: List[str], top_n: int = 20, figsize: Tuple[int,int]=(8,6)) -> None:
    """Feature importance を棒グラフで表示する。"""
    if model is None:
        raise ValueError('model is None')
    try:
        importances = model.feature_importance(importance_type='gain')
    except Exception:
        # fallback for sklearn wrapper
        importances = model.booster_.feature_importance(importance_type='gain')

    fi = pd.DataFrame({'feature': feature_names, 'importance': importances})
    fi = fi.sort_values('importance', ascending=False).head(top_n)

    plt.figure(figsize=figsize)
    plt.barh(fi['feature'][::-1], fi['importance'][::-1])
    plt.xlabel('Importance (gain)')
    plt.title('Feature Importance')
    plt.tight_layout()
    plt.show()


def generate_bet_strategy(horses: pd.DataFrame, win_prob_col: str = 'win_prob', odds_col: str = 'odds',
                          bankroll: float = 50000.0, min_ev: float = 1.2, half_kelly: bool = True) -> pd.DataFrame:
    """勝率とオッズから購入戦略を生成する。

    horses: DataFrame に少なくとも次のカラムがあることを期待する:
      - 'horse_no' または 'no'
      - 'horse_name' または 'name'
      - 系統情報（例: 'lineage_group'）
      - win_prob_col (予測勝率、0-1)
      - odds_col (現在のオッズ、小数)

    出力DataFrameに以下を付与して返す:
      ['horse_no','horse_name','lineage_group','win_prob','expected_value','recommended','stake']
    """
    df = horses.copy()
    # normalize column names
    if 'horse_no' not in df.columns and 'no' in df.columns:
        df = df.rename(columns={'no':'horse_no'})
    if 'horse_name' not in df.columns and 'name' in df.columns:
        df = df.rename(columns={'name':'horse_name'})

    p = df[win_prob_col].astype(float)
    odds = df[odds_col].astype(float)
    ev = p * odds
    df['win_prob'] = p
    df['odds'] = odds
    df['expected_value'] = ev
    df['recommended'] = ev >= min_ev

    stakes = []
    for pi, oi in zip(p, odds):
        # decimal odds -> b = odds - 1
        b = oi - 1.0
        q = 1.0 - pi
        if b <= 0:
            f = 0.0
        else:
            f = (b * pi - q) / b  # Kelly fraction
        if f <= 0:
            stake = 0.0
        else:
            if half_kelly:
                f = f * 0.5
            f = max(0.0, min(1.0, f))
            stake = f * bankroll
        stakes.append(stake)

    df['stake'] = stakes

    out_cols = []
    for c in ['horse_no','horse_name','lineage_group','win_prob','expected_value','recommended','stake']:
        if c in df.columns:
            out_cols.append(c)

    out = df[out_cols].copy()
    # 整形
    if 'stake' in out.columns:
        out['stake'] = out['stake'].round(0).astype(int)
    if 'expected_value' in out.columns:
        out['expected_value'] = out['expected_value'].round(3)

    return out.sort_values('expected_value', ascending=False)


if __name__ == '__main__':
    print('このモジュールはインポートして使用してください。利用例はファイルの先頭ドキュメントを参照。')
