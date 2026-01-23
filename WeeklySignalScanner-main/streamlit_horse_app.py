import streamlit as st
import pandas as pd
try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except Exception:
    plt = None
    HAS_MATPLOTLIB = False
import requests
import io
import datetime
from pathlib import Path

from horse_model import HorseDataProcessor, train_lgbm_model, plot_feature_importance, generate_bet_strategy


st.set_page_config(page_title='競馬 血統分析AI (Streamlit)', layout='wide')

st.title('競馬 血統分析AI — Streamlit フロントエンド')

st.markdown('ファイルをアップロードして、前処理・学習・予想をボタンで実行できます。')

with st.sidebar:
    st.header('設定')
    father_col = st.text_input('父馬名カラム', value='father_name')
    track_col = st.text_input('馬場カラム', value='track_condition')
    target_col = st.text_input('着順カラム (勝ち=1と判定)', value='着順')
    prev_rank_col = st.text_input('前走着順カラム', value='prev_rank')
    weight_col = st.text_input('斤量カラム', value='斤量')
    odds_col = st.text_input('オッズカラム', value='odds')
    bankroll = st.number_input('軍資金 (円)', value=50000)
    min_ev = st.number_input('期待値閾値 (例:1.2)', value=1.2)

st.markdown('またはネット上のCSVを指定して自動取得できます。')
url_results = st.text_input('レース結果CSVのURL', value='')
url_pedigree = st.text_input('血統（ペディグリー）CSVのURL', value='')

uploaded_results = st.file_uploader('レース結果CSVをアップロード', type=['csv'])
uploaded_pedigree = st.file_uploader('血統（ペディグリー）CSVをアップロード', type=['csv'])

@st.cache_data(show_spinner=False)
def fetch_table_to_df(url: str) -> pd.DataFrame:
    """URLからCSVまたはParquetを取得してDataFrameにする。"""
    if not url:
        return pd.DataFrame()
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        content = resp.content
        # 簡易判定: URL末尾の拡張子で判定
        if url.lower().endswith('.parquet'):
            return pd.read_parquet(io.BytesIO(content))
        # try CSV first, fallback to parquet
        try:
            return pd.read_csv(io.BytesIO(content))
        except Exception:
            return pd.read_parquet(io.BytesIO(content))
    except Exception as e:
        st.error(f'URL取得エラー: {url} -> {e}')
        return pd.DataFrame()

if 'processor' not in st.session_state:
    st.session_state['processor'] = HorseDataProcessor()

proc: HorseDataProcessor = st.session_state['processor']

def load_and_show():
    # Prefer URL if provided, else uploaded files
    if url_results and url_pedigree:
        df_res = fetch_table_to_df(url_results)
        df_ped = fetch_table_to_df(url_pedigree)
        if df_res.empty or df_ped.empty:
            st.warning('URLからの取得に失敗しました。ファイルアップロードをお試しください。')
            return
        df = pd.merge(df_res, df_ped, how='left', on='horse_id')
        st.success('URLからCSVを取得しました。')
    else:
        if uploaded_results is None or uploaded_pedigree is None:
            st.warning('結果CSVと血統CSVの両方をアップロードするか、URLを指定してください。')
            return
        try:
            # handle uploaded file-like objects
            uploaded_results.seek(0)
            uploaded_pedigree.seek(0)
            df = proc.load_and_merge_results_pedigree(uploaded_results, uploaded_pedigree, on='horse_id')
        except Exception:
            # fallback: try merge on 'horse_id' robustly
            uploaded_results.seek(0)
            uploaded_pedigree.seek(0)
            df_res = pd.read_csv(uploaded_results)
            df_ped = pd.read_csv(uploaded_pedigree)
            df = pd.merge(df_res, df_ped, how='left', on='horse_id')

    st.session_state['df_raw'] = df
    st.success('CSVを読み込みました — 行数: {}'.format(len(df)))
    st.dataframe(df.head(200))

if st.button('CSV読み込み・表示'):
    load_and_show()

# オフライン検証用: サンプルデータを読み込む
if st.button('サンプルデータを読み込む (オフライン用)'):
    project_root = Path(__file__).resolve().parent.parent
    sample_res = project_root / 'samples' / 'sample_horse_results.csv'
    sample_ped = project_root / 'samples' / 'sample_pedigree.csv'
    try:
        df = proc.load_and_merge_results_pedigree(str(sample_res), str(sample_ped), on='horse_id')
        st.session_state['df_raw'] = df
        st.success('サンプルデータを読み込みました')
        st.dataframe(df.head(200))
    except Exception as e:
        st.error('サンプルデータの読み込みに失敗しました: ' + str(e))

# Netkeiba スクレイプ UI
with st.sidebar.expander('Netkeiba スクレイプ (Parquet 保存)', expanded=False):
    st.write('netkeiba のレース結果を取得して Parquet に保存します。')
    default_start = datetime.date.today() - datetime.timedelta(days=365*5)
    default_end = datetime.date.today()
    nb_start = st.date_input('開始日', value=default_start)
    nb_end = st.date_input('終了日', value=default_end)
    out_default = str(Path.cwd() / 'data' / 'netkeiba' / f'netkeiba_{nb_start.isoformat()}_{nb_end.isoformat()}.parquet')
    nb_out = st.text_input('出力Parquetパス', value=out_default)
    nb_sleep = st.number_input('リクエスト間スリープ(秒)', value=1.0, min_value=0.0)
    nb_max = st.number_input('最大レース数(テスト、0は無制限)', value=0, min_value=0)
    if st.button('Netkeiba から取得・保存'):
        import subprocess, os
        os.makedirs(str(Path(nb_out).parent), exist_ok=True)
        script_path = str(Path(__file__).resolve().parent / 'scripts' / 'netkeiba_scraper.py')
        python_bin = os.path.abspath('/workspaces/WeeklySignalScanner-main/.venv/bin/python')
        cmd = f"{python_bin} {script_path} --start-date {nb_start.isoformat()} --end-date {nb_end.isoformat()} --out {nb_out} --sleep {nb_sleep}"
        if nb_max and nb_max > 0:
            cmd += f" --max-races {int(nb_max)}"
        try:
            env = os.environ.copy()
            # Ensure the project root is importable so `from horse_model import ...` works
            env['PYTHONPATH'] = str(Path(__file__).resolve().parent)
            subprocess.Popen(cmd, shell=True, cwd=str(Path(__file__).resolve().parent), env=env)
            st.success(f'取得ジョブを開始しました。出力: {nb_out} (背景実行)')
        except Exception as e:
            st.error(f'ジョブ起動失敗: {e}')

# 取得済み Parquet を読み込んでレース毎に表示する UI
with st.sidebar.expander('Netkeiba: 取得済み Parquet をロードして確認', expanded=False):
    import glob
    parquet_files = sorted(glob.glob('outputs/*.parquet'))
    if not parquet_files:
        st.write('outputs/*.parquet が見つかりません')
    else:
        sel_parquet = st.selectbox('Parquet ファイルを選択', parquet_files)
        if st.button('ファイルを読み込む'):
            try:
                df_scraped = pd.read_parquet(sel_parquet)
                st.session_state['df_scraped'] = df_scraped
                st.success(f'読み込みました: {sel_parquet} （行数: {len(df_scraped)}）')
            except Exception as e:
                st.error(f'読み込み失敗: {e}')

st.markdown('---')

if 'df_raw' in st.session_state:
    df = st.session_state['df_raw']

    if st.button('前処理: 系統マッピング・馬場数値化'):
        df_proc = proc.process_lineage_and_conditions(df, father_col=father_col, track_col=track_col)
        # 作成する勝ちフラグ
        if target_col in df_proc.columns:
            df_proc['is_win'] = (df_proc[target_col] == 1).astype(int)
        else:
            st.warning(f'指定した着順カラム {target_col} が見つかりません。is_win カラムは作成されません。')
        st.session_state['df_proc'] = df_proc
        st.success('前処理完了')
        st.dataframe(df_proc.head(200))

    if 'df_proc' in st.session_state:
        df_proc = st.session_state['df_proc']

        if st.button('系統をターゲットエンコーディング（学習）'):
            if 'is_win' not in df_proc.columns:
                st.error('is_win カラムが必要です。まず前処理で着順カラムを指定して is_win を作成してください。')
            else:
                df_te, enc = proc.fit_target_encode(df_proc, cat_col='lineage_group', target_col='is_win')
                st.session_state['df_te'] = df_te
                st.session_state['encoder_lineage'] = enc
                st.success('ターゲットエンコーディングを適用しました。')
                st.dataframe(df_te[['lineage_group','lineage_group_te']].drop_duplicates().head(200))

    if 'df_te' in st.session_state:
        df_te = st.session_state['df_te']

        st.markdown('### 学習用特徴量選択')
        default_features = ['lineage_group_te']
        if prev_rank_col in df_te.columns:
            default_features.append(prev_rank_col)
        if weight_col in df_te.columns:
            default_features.append(weight_col)
        if 'track_numeric' in df_te.columns:
            default_features.append('track_numeric')

        features = st.multiselect('特徴量', options=list(df_te.columns), default=default_features)

        if st.button('LightGBMで学習実行'):
            if 'is_win' not in df_te.columns:
                st.error('is_win がありません。前処理を確認してください。')
            else:
                # 必要なカラムを埋める簡易処理
                Xdf = df_te.copy()
                for c in features:
                    if Xdf[c].isnull().any():
                        if Xdf[c].dtype.kind in 'biufc':
                            Xdf[c] = Xdf[c].fillna(Xdf[c].median())
                        else:
                            Xdf[c] = Xdf[c].fillna('その他')

                try:
                    model, feature_names = train_lgbm_model(Xdf, features=features, target='is_win')
                except Exception as e:
                    st.error('学習中にエラーが発生しました: ' + str(e))
                else:
                    st.session_state['model'] = model
                    st.success('学習完了')
                    st.write('学習に使用した特徴量:', feature_names)
                    # matplotlib が無い環境ではプロットできないので代替表示
                    if HAS_MATPLOTLIB:
                        try:
                            st.pyplot(plot_feature_importance(model, feature_names))
                        except Exception as e:
                            st.error(f'プロット表示中にエラーが発生しました: {e}')
                    else:
                        try:
                            try:
                                importances = model.feature_importance(importance_type='gain')
                            except Exception:
                                importances = model.booster_.feature_importance(importance_type='gain')
                            fi = pd.DataFrame({'feature': feature_names, 'importance': importances})
                            fi = fi.sort_values('importance', ascending=False).head(20).reset_index(drop=True)
                            st.info('matplotlib がインストールされていないため、上位特徴量を表で表示します。`pip install matplotlib` でチャート表示が可能です。')
                            st.dataframe(fi)
                        except Exception as e:
                            st.error(f'特徴量重要度の表示に失敗しました: {e}')

    if 'model' in st.session_state and 'df_te' in st.session_state:
        model = st.session_state['model']
        df_te = st.session_state['df_te']

        if st.button('予測・買い目生成'): 
            # 予測
            features = [c for c in df_te.columns if c.endswith('_te') or c in [prev_rank_col, weight_col, 'track_numeric']]
            Xpred = df_te[features].copy()
            for c in Xpred.columns:
                if Xpred[c].isnull().any():
                    if Xpred[c].dtype.kind in 'biufc':
                        Xpred[c] = Xpred[c].fillna(Xpred[c].median())
                    else:
                        Xpred[c] = Xpred[c].fillna('その他')

            # lightgbm の scikit-learn wrapper では predict_proba があるが、ここでは model.predict を利用
            try:
                win_prob = model.predict(Xpred)
            except Exception:
                # fallback: if train_lgbm returned native booster
                win_prob = model.predict(Xpred)

            df_te['win_prob'] = win_prob

            if odds_col not in df_te.columns:
                st.error(f'オッズカラム {odds_col} が見つかりません。CSVにオッズ列を含めてください。')
            else:
                bets = generate_bet_strategy(df_te.assign(odds=df_te[odds_col]), win_prob_col='win_prob', odds_col='odds', bankroll=bankroll, min_ev=min_ev)
                st.subheader('推奨買い目')
                st.dataframe(bets)

else:
    st.info('まずは左のサイドバーでカラム設定を確認し、CSVをアップロードしてください。')

# 取得済みスクレイプ結果の詳細表示
if 'df_scraped' in st.session_state:
    df_s = st.session_state['df_scraped']
    st.markdown('## 取得済み Netkeiba データの確認')
    st.write(f'行数: {len(df_s)}')
    # レースごとに分割した per-race ファイルがあれば、日付→レース で選べるようにする
    from pathlib import Path
    per_race_summary = Path('outputs') / 'predictions_by_race' / 'summary_by_race.csv'
    if per_race_summary.exists():
        try:
            summary = pd.read_csv(per_race_summary)
            dates = sorted(summary['race_date'].dropna().unique())
            sel_date = st.selectbox('レース日で絞り込む', ['(全て)'] + list(dates))
            if sel_date and sel_date != '(全て)':
                subset = summary[summary['race_date'] == sel_date]
            else:
                subset = summary

            # show race selector
            options = []
            for _, r in subset.iterrows():
                # attempt to read a single row from the per-race file to get race title / class
                race_title = ''
                race_grade = ''
                file_rel = r['file']
                try:
                    p = Path(file_rel)
                    if not p.exists():
                        p = Path.cwd() / file_rel
                    if p.exists():
                        tmp = pd.read_parquet(p, columns=[c for c in ['race_class'] if c in pd.read_parquet(p).columns]).head(1)
                        # race_class available
                        if 'race_class' in tmp.columns:
                            race_grade = str(tmp['race_class'].iloc[0])
                        # try common title columns
                        pq = pd.read_parquet(p)
                        for tc in ['race_name', 'race_title', 'title', 'name']:
                            if tc in pq.columns and pq[tc].notna().any():
                                race_title = str(pq[tc].dropna().unique()[0])
                                break
                except Exception:
                    race_title = ''
                # build display: show URL (short) + '_' + title (if exists) + ' ['+grade+']'
                short_url = r['race_url'] if pd.notna(r['race_url']) else ''
                display = f"{short_url} _{race_title}"
                if race_grade:
                    display = f"{display} [{race_grade}]"
                options.append((display, r['file']))

            sel_display = st.selectbox('レースファイルを選択', ['(一覧表示)'] + [d for d, _ in options])
            if sel_display and sel_display != '(一覧表示)':
                # find file matching display
                file_rel = None
                for d, f in options:
                    if d == sel_display:
                        file_rel = f
                        break
                if file_rel:
                    p = Path(file_rel)
                    if not p.exists():
                        # try relative to project root
                        p = Path.cwd() / file_rel
                    try:
                        df_filtered = pd.read_parquet(p)
                        st.success(f'読み込みました: {p} （行数: {len(df_filtered)}）')
                    except Exception as e:
                        st.error(f'レースファイルの読み込みに失敗しました: {e}')
                        df_filtered = df_s
                else:
                    df_filtered = df_s
            else:
                # 一覧表示: filter by date if selected
                if sel_date and sel_date != '(全て)':
                    # show combined rows for that date from summary files
                    rows = []
                    for _, r in subset.iterrows():
                        p = Path(r['file'])
                        if not p.exists():
                            p = Path.cwd() / r['file']
                        try:
                            d = pd.read_parquet(p)
                            rows.append(d)
                        except Exception:
                            continue
                    if rows:
                        df_filtered = pd.concat(rows, ignore_index=True)
                    else:
                        df_filtered = df_s
                else:
                    df_filtered = df_s
        except Exception as e:
            st.error('per-race summary の読み込みに失敗しました: ' + str(e))
            df_filtered = df_s
    else:
        # fallback: original behavior filtering the single loaded parquet by columns
        df_filtered = df_s
        if 'race_date' in df_s.columns:
            dates = sorted(df_s['race_date'].dropna().unique())
            sel_date = st.selectbox('レース日で絞り込む', ['(全て)'] + dates)
            if sel_date and sel_date != '(全て)':
                df_filtered = df_s[df_s['race_date'] == sel_date]
        if 'race_url' in df_filtered.columns:
            urls = list(df_filtered['race_url'].dropna().unique())
            sel_url = st.selectbox('レースURLを選択', ['(全て)'] + urls)
            if sel_url and sel_url != '(全て)':
                df_filtered = df_filtered[df_filtered['race_url'] == sel_url]

    # remove race_url column from displayed table to reduce clutter
    if 'race_url' in df_filtered.columns:
        display_df = df_filtered.drop(columns=['race_url'])
    else:
        display_df = df_filtered
    st.dataframe(display_df.reset_index(drop=True).head(500))
