import streamlit as st
import pandas as pd
import os
import glob

st.set_page_config(page_title='Netkeiba Predictions', layout='wide')
st.title('Netkeiba: Predictions & Betting Suggestions')

pred_path = 'outputs/netkeiba_predictions_with_probs.parquet'
bets_path = 'outputs/netkeiba_predictions.parquet'
# support per-day partitioned predictions
# resolve predictions directory from multiple likely locations (cwd or repo root)
PRED_DIR = 'outputs/predictions_by_date'
if not os.path.isdir(PRED_DIR):
    # try parent folder (when this script lives in WeeklySignalScanner-main/WeeklySignalScanner-main)
    try:
        base = os.path.dirname(__file__)
        alt = os.path.normpath(os.path.join(base, '..', 'outputs', 'predictions_by_date'))
        if os.path.isdir(alt):
            PRED_DIR = alt
    except Exception:
        pass
if not os.path.isdir(PRED_DIR):
    # try workspace root outputs (when run from repository root)
    alt2 = os.path.join(os.getcwd(), 'outputs', 'predictions_by_date')
    if os.path.isdir(alt2):
        PRED_DIR = alt2

# discover available dates (combine index.csv and parquet filenames to avoid stale index)
available_dates = []
if os.path.isdir(PRED_DIR):
    # from index.csv if present
    idx_path = os.path.join(PRED_DIR, 'index.csv')
    idx_dates = []
    if os.path.exists(idx_path):
        try:
            df_idx = pd.read_csv(idx_path)
            if 'race_date' in df_idx.columns:
                idx_dates = list(df_idx['race_date'].astype(str).unique())
        except Exception:
            idx_dates = []
    # from parquet filenames
    parts = sorted(glob.glob(os.path.join(PRED_DIR, 'predictions_*.parquet')))
    file_dates = [os.path.basename(p).replace('predictions_','').replace('.parquet','') for p in parts]
    # merge and sort unique
    merged = sorted(set(idx_dates) | set(file_dates))
    available_dates = merged
# also include dates available in the master predictions file if present
try:
    if os.path.exists(pred_path):
        df_master = pd.read_parquet(pred_path)
        if 'race_date' in df_master.columns:
            master_dates = set(df_master['race_date'].astype(str).unique())
            available_dates = sorted(set(available_dates) | master_dates)
except Exception:
    pass

# UI: choose date or Combined
options = ['Combined'] + available_dates if available_dates else ['Combined']
sel_date = st.selectbox('日付でフィルタ (Combined は全件表示)', options, index=0)

# determine which prediction file to load
if sel_date == 'Combined':
    load_pred = pred_path
else:
    load_pred = os.path.join(PRED_DIR, f'predictions_{sel_date}.parquet')

reload_btn = st.button('Reload')
if reload_btn:
    # Avoid calling Streamlit runtime internals to preserve compatibility across
    # Streamlit versions and environments. Instruct user to refresh the browser.
    st.info('ページをブラウザで再読み込みしてください（互換性のため自動再実行は無効化されています）。')

df = None
if sel_date == 'Combined':
    if pd.io.common.file_exists(pred_path):
        df = pd.read_parquet(pred_path)
    else:
        st.warning('マスター予測ファイルが見つかりません: ' + str(pred_path))
        df = pd.DataFrame()
else:
    # try per-day file first
    if pd.io.common.file_exists(load_pred):
        try:
            df_day = pd.read_parquet(load_pred)
            # quick quality checks: must have at least one horse_name and at least one race_url with /race/
            ok_horse = ('horse_name' in df_day.columns) and (df_day['horse_name'].notna().sum() > 0)
            ok_race = ('race_url' in df_day.columns) and (df_day['race_url'].astype(str).str.contains('/race/').sum() > 0)
            if ok_horse and ok_race:
                df = df_day
            else:
                # fallback to master predictions if available
                if pd.io.common.file_exists(pred_path):
                    df_master = pd.read_parquet(pred_path)
                    df_master_sel = df_master[df_master['race_date'] == sel_date]
                    if len(df_master_sel) > 0:
                        df = df_master_sel
                    else:
                        st.warning(f'日別ファイルは品質不良のため使用できません: {load_pred}。マスターにも該当日がありません。')
                        df = df_day
                else:
                    st.warning(f'日別ファイルは品質不良のため使用できません: {load_pred}。マスターが見つかりません。')
                    df = df_day
        except Exception as e:
            st.warning('日別予測ファイルの読み込みに失敗しました: ' + str(e))
            df = pd.DataFrame()
    else:
        # no per-day file: try master
        if pd.io.common.file_exists(pred_path):
            df_master = pd.read_parquet(pred_path)
            df_master_sel = df_master[df_master['race_date'] == sel_date]
            if len(df_master_sel) > 0:
                df = df_master_sel
            else:
                st.warning('該当日付の予測が見つかりません: ' + sel_date)
                df = pd.DataFrame()
        else:
            st.warning('該当日付の予測ファイルが見つかりません: ' + load_pred)
            df = pd.DataFrame()
    # derive bets for this selection if possible (expected_value present)
    # normalize columns: ensure horse_name and odds_display exist
    if 'horse_name' not in df.columns and 'horse' in df.columns:
        df['horse_name'] = df['horse']
    if 'horse_name' not in df.columns:
        df['horse_name'] = df.get('horse_name').fillna('不明') if 'horse_name' in df.columns else '不明'
    # choose numeric odds for display
    if 'odds_f' in df.columns:
        df['odds_display'] = pd.to_numeric(df['odds_f'], errors='coerce')
    elif 'odds' in df.columns:
        df['odds_display'] = pd.to_numeric(df['odds'], errors='coerce')
    else:
        df['odds_display'] = pd.NA

    if 'expected_value' in df.columns:
        # select bets meeting threshold
        raw_bets = df[df['expected_value'] >= 1.10].copy()
        raw_bets = raw_bets.sort_values(['race_url','expected_value'], ascending=[True, False])
        raw_bets = raw_bets.groupby('race_url').head(3).reset_index(drop=True)
    else:
        raw_bets = pd.DataFrame()
    # aggregate per race_url into one row for display
    if not raw_bets.empty:
        def aggr_horses(g):
            items = []
            for _, r in g.iterrows():
                name = r.get('horse_name') or '不明'
                odds = r.get('odds_display')
                ev = r.get('expected_value')
                stake = r.get('stake', 0)
                items.append(f"{name} ({odds}) EV={ev:.2f} stake={int(stake)}")
            return '; '.join(items)
        bets = raw_bets.groupby('race_url').apply(lambda g: pd.Series({
            'race_date': g['race_date'].iloc[0] if 'race_date' in g.columns else None,
            'horses': aggr_horses(g),
            'total_stake': int(g['stake'].sum()) if 'stake' in g.columns else 0,
            'count': len(g)
        })).reset_index()
    else:
        bets = pd.DataFrame()

    st.subheader('Predictions (sample)')
    # 日本語ヘッダーに変換して表示
    col_map = {
        'race_date':'開催日', 'race_url':'レースURL', 'horse_name':'馬名', 'rank':'着順', 'jockey':'騎手',
        'odds':'オッズ', 'win_prob':'勝率', 'expected_value':'期待値', 'recent_ranks':'近走着順', 'age_sex':'年齢・性別',
        'father_group':'系統', 'father':'父', 'damsire':'母父', 'trainer':'調教師',
        'surface':'芝/ダート', 'distance':'距離', 'course_direction':'コース形態', 'condition':'馬場状態',
        'odds_f':'オッズ(数値)', 'recent_mean_rank':'近走平均着順', 'age':'年齢'
    }
    display_cols = ['race_date','race_url','horse_name','odds','win_prob','expected_value','recent_ranks','age_sex','father_group']
    # prepare odds_display numeric and sanitize obvious invalid values (years etc.)
    if 'odds_display' not in df.columns:
        if 'odds_f' in df.columns:
            df['odds_display'] = pd.to_numeric(df['odds_f'], errors='coerce')
        elif 'odds' in df.columns:
            df['odds_display'] = pd.to_numeric(df['odds'], errors='coerce')
        else:
            df['odds_display'] = pd.NA
    # sanitize: treat values >=1000 or <=0 as missing
    df.loc[df['odds_display'].notna() & ((df['odds_display'] >= 1000) | (df['odds_display'] <= 0)), 'odds_display'] = pd.NA

    df_show = df.copy()
    # remove raw race_url from the table display; links will be shown at side
    display_cols = [c for c in display_cols if c in df_show.columns and c != 'race_url']
    # prefer odds_display for shown odds
    if 'odds_display' in df_show.columns:
        # ensure we show it under key 'odds' to keep mapping simple
        df_show['odds'] = df_show['odds_display']
    df_show = df_show[[c for c in display_cols if c in df_show.columns]]
    df_show = df_show.rename(columns={k:v for k,v in col_map.items() if k in df_show.columns})

    # layout: table on left, list of race links on right
    # prepare unique races in order
    # prefer race links (contain '/race/'), but keep others for diagnostics
    all_urls = df[['race_url','race_date']].drop_duplicates()
    # mark valid race urls that point to a race page
    def is_valid_race_url(u):
        try:
            return '/race/' in str(u)
        except Exception:
            return False
    all_urls['is_race'] = all_urls['race_url'].apply(is_valid_race_url)
    uniq = all_urls.sort_values(['race_date','race_url'])
    # try to sort by race_date then race_url
    if 'race_date' in uniq.columns:
        uniq = uniq.sort_values(['race_date','race_url'])
    else:
        uniq = uniq.sort_values('race_url')

    # ensure rank is 1-based for display
    if 'rank' in df_show.columns:
        try:
            df_show['rank'] = pd.to_numeric(df_show['rank'], errors='coerce')
            # add 1 to convert 0-based -> 1-based where numeric
            df_show.loc[df_show['rank'].notna(), 'rank'] = df_show.loc[df_show['rank'].notna(), 'rank'] + 1
            # convert back to int where possible
            df_show['rank'] = df_show['rank'].dropna().astype('Int64')
        except Exception:
            pass

    left, right = st.columns([5, 1])
    with left:
        st.subheader('Race-separated Predictions')
        # iterate races and display each race separately for clarity
        if not uniq.empty:
            for _, rinfo in uniq.iterrows():
                race_url = rinfo['race_url']
                race_date = rinfo.get('race_date')
                header = f"**開催日: {race_date} — レース: {race_url}**"
                if not rinfo.get('is_race'):
                    header += '  *(警告: このリンクはレースページではない可能性があります)*'
                st.markdown(header)
                # select rows for this race
                chunk_idx = df['race_url'] == race_url
                chunk = df_show[chunk_idx].copy()
                # remove rows with missing horse names (likely corrupted rows)
                if '馬名' in chunk.columns:
                    chunk = chunk[chunk['馬名'].notna() & (chunk['馬名'] != '')]
                elif 'horse_name' in chunk.columns:
                    chunk = chunk[chunk['horse_name'].notna() & (chunk['horse_name'] != '')]
                # sort by rank if available
                if 'rank' in chunk.columns:
                    try:
                        chunk['rank'] = pd.to_numeric(chunk['rank'], errors='coerce')
                        chunk = chunk.sort_values('rank')
                    except Exception:
                        pass
                if not chunk.empty:
                    st.table(chunk)
                else:
                    st.write('このレースの表示対象行は欠損しています（馬名や主要列が欠けている可能性があります）。')
        else:
            st.dataframe(df_show)

    with right:
        st.subheader('Race Links')
        # unique race urls for this selection
        if 'race_url' in df.columns:
            for _, r in uniq.iterrows():
                url = r['race_url']
                date = r.get('race_date')
                # show short text and clickable link
                try:
                    st.markdown(f"- [{date}]({url})")
                except Exception:
                    st.write(url)

    st.subheader('Bet Suggestions')
    if bets.empty:
        st.info('現時点では期待値 >= 1.10 の推奨馬はありません。各レースの top1 を表示します。')
        # show top1 for this selection (one per race)
        top1 = df.sort_values('win_prob', ascending=False).groupby('race_url').head(1)
        if not top1.empty:
            top1 = top1.copy()
            top1['horse_name'] = top1['horse_name'].fillna('不明')
            top1['odds_display'] = top1.get('odds_display')
            tmap = {'race_date':'開催日','race_url':'レースURL','horse_name':'馬名','odds_display':'オッズ','win_prob':'勝率','expected_value':'期待値'}
            top1 = top1.rename(columns={k:v for k,v in tmap.items() if k in top1.columns})
            # build safe column list after rename
            desired_keys = ['race_url','race_date','horse_name','odds_display','win_prob','expected_value']
            col_order = []
            # map keys to their renamed equivalents if present
            for k in desired_keys:
                col_name = tmap.get(k, k)
                if col_name in top1.columns:
                    col_order.append(col_name)
                elif k in top1.columns:
                    col_order.append(k)
            if not col_order:
                st.table(top1)
            else:
                st.table(top1[col_order])
    else:
        # display aggregated bets per race
        bets_show = bets.copy()
        bets_show = bets_show.rename(columns={'race_url':'レースURL','race_date':'開催日','horses':'推奨馬(複数)','total_stake':'合計購入額','count':'件数'})
        st.table(bets_show[['レースURL','開催日','推奨馬(複数)','合計購入額','件数']])

    st.subheader('ルール（要約）')
    st.markdown('- 資金: 1レースあたり 1000 円')
    st.markdown('- 推奨基準: 期待値 (win_prob * odds) >= 1.10')
    st.markdown('- 1レース最大 3 点まで、該当馬が無ければ購入しない')
    st.markdown('- ステークは選択馬で均等分割')

    st.subheader('予想の根拠（自動生成）')
    with st.expander('Explain for top horses'):
        def qualitative_recent(ranks):
            try:
                if ranks is None:
                    return '近走データなし'
                # ranks may be list-like or string
                import ast
                if isinstance(ranks, str):
                    try:
                        v = ast.literal_eval(ranks)
                        ranks = v
                    except Exception:
                        # extract digits
                        import re
                        m = re.findall(r"\d+", ranks)
                        ranks = [int(x) for x in m] if m else []
                ranks = [int(x) for x in ranks] if ranks is not None else []
                if not ranks:
                    return '近走データなし'
                top3 = sum(1 for x in ranks if x <= 3)
                last = ranks[0] if len(ranks) > 0 else None
                if top3 >= max(1, len(ranks)//2):
                    return f'直近{len(ranks)}戦で3着以内が多い（{top3}回）'
                if last and last == 1:
                    return '直近で勝利あり'
                if top3 == 0:
                    return '直近で掲示板外が多い'
                return f'直近{len(ranks)}戦で安定した成績（上位{top3}回）'
            except Exception:
                return '近走データ不明'

        def age_group(age_sex):
            try:
                if not age_sex:
                    return '年齢情報なし'
                import re
                m = re.search(r"(\d{1,2})", str(age_sex))
                if not m:
                    return '年齢情報なし'
                age = int(m.group(1))
                if age <= 3:
                    return '若手'
                if 4 <= age <= 6:
                    return '適齢期'
                return '高齢'
            except Exception:
                return '年齢情報不明'

        def odds_label(odds):
            try:
                if odds is None:
                    return 'オッズ不明'
                o = float(odds)
                if o <= 3:
                    return '人気'
                if o <= 15:
                    return '中穴'
                return '大穴'
            except Exception:
                return 'オッズ不明'

        # helper to produce natural language rationale by comparing to race field
        def rationale(row, df_all):
            parts = []
            race = df_all[df_all['race_url'] == row['race_url']]
            # recent
            parts.append(qualitative_recent(row.get('recent_ranks')))
            # age
            parts.append('年齢: ' + age_group(row.get('age_sex')))
            # odds
            parts.append('オッズ状況: ' + odds_label(row.get('odds_display') if 'odds_display' in row else row.get('odds')))
            # relative features
            try:
                if 'recent_mean_rank' in race.columns and row.get('recent_mean_rank') is not None:
                    median = race['recent_mean_rank'].median()
                    if pd.notna(median):
                        if row.get('recent_mean_rank') < median:
                            parts.append('近走平均が同レースより良好')
                        else:
                            parts.append('近走平均は同レースで標準かやや不利')
            except Exception:
                pass
            try:
                if 'odds_display' in race.columns and row.get('odds_display') is not None:
                    med = race['odds_display'].median()
                    if pd.notna(med):
                        if row.get('odds_display') < med:
                            parts.append('現在は人気寄り')
                        else:
                            parts.append('人気薄')
            except Exception:
                pass
            return parts

        for i, r in df.sort_values('expected_value', ascending=False).head(10).iterrows():
            name = r.get('horse_name') or '不明'
            win_prob = r.get('win_prob')
            ev = r.get('expected_value')
            st.markdown(f"**{name}** — 勝率推定: {win_prob:.3f} 期待値: {ev:.3f}" if win_prob is not None and ev is not None else f"**{name}**")
            # show recent race short list
            rr = r.get('recent_ranks')
            st.markdown(f"- 近走: {rr}")
            # generate rationale bullets
            parts = rationale(r, df)
            for p in parts:
                st.markdown('- ' + p)
            st.markdown('---')

    st.caption('注意: モデルは簡易版です。追加データ・長期学習で精度向上します。')
