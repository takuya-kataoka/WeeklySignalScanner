"""
netkeiba_scraper.py

netkeiba のレース結果ページから過去のレース結果を取得し、
各馬の父・母父を取得して系統グループを付与、
最終的に pandas.DataFrame を pyarrow エンジンで Parquet 保存するスクリプト。

使い方例:
    python netkeiba_scraper.py --start-date 2021-01-01 --end-date 2025-12-31 --out results_parquet.parquet

注意:
 - netkeiba のページ構造は変わる可能性があります。パーサは汎用的に実装していますが
     必要に応じて CSS セレクタを調整してください。
 - 過度なアクセスはサーバに負荷をかけます。`--sleep` で間隔を空けてください。
"""

import argparse
import datetime
import time
import re
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

from horse_model import HorseDataProcessor
import os
import json
from datetime import datetime as _dt


USER_AGENT = 'Mozilla/5.0 (compatible; NetKeibaScraper/1.0; +https://example.com)'


def daterange(start_date: datetime.date, end_date: datetime.date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def fetch_url(session: requests.Session, url: str, timeout: int = 20) -> Optional[str]:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        # set encoding from apparent_encoding (db.netkeiba often uses EUC-JP/Shft-JIS)
        try:
            if r.apparent_encoding:
                r.encoding = r.apparent_encoding
        except Exception:
            pass
        return r.text
    except Exception:
        return None


def find_race_links_on_date(html: str) -> List[str]:
    """日付ページの HTML からレース結果ページへのリンクを抽出する（汎用的）。"""
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # レース結果ページに絞り込む:
        # - パスに '/race/' または 'race/result' や 'race_id' を含む
        # - ただし jockey 関連や馬ページそのものは除外する
        low = href.lower()
        if ('/jockey/' in low) or ('/person/' in low):
            continue
        if (('/race/' in low) or ('race/result' in low) or ('race_id' in low) or ('race_id=' in low)) and ('horse' not in low):
            links.append(href)
    # uniq
    return list(dict.fromkeys(links))


def parse_race_result(html: str, base_url: str):
    """レース結果ページ HTML を解析して馬ごとの基本情報と馬ページURLを返す。

    返却する dict 要素: horse_name, horse_url, rank, jockey, weight, odds (可能なら), horse_no
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    # extract race meta: class, surface, distance, course direction, condition
    race_meta = {'race_class': None, 'surface': None, 'distance': None, 'course_direction': None, 'condition': None}
    try:
        # look for title/header containing race descriptor
        title = None
        h = soup.find(['h1', 'h2'])
        if h:
            title = h.get_text(strip=True)
        if not title:
            title = soup.title.string if soup.title else None
        if title:
            # surface + distance e.g. '芝1600m' or 'ダート1200m'
            m = re.search(r'(芝|ダート)\s*(\d+)m', title)
            if m:
                race_meta['surface'] = m.group(1)
                race_meta['distance'] = int(m.group(2))
            # course direction heuristics
            if '右' in title or '右回り' in title:
                race_meta['course_direction'] = '右'
            if '左' in title or '左回り' in title:
                race_meta['course_direction'] = '左'
            # race class
            cls = None
            for token in ['G1', 'G2', 'G3', 'オープン', '1勝', '2勝', '未勝利', '特別']:
                if token in title:
                    cls = token
                    break
            race_meta['race_class'] = cls
        # condition: look for '良', '稍重', '重', '不良'
        body = soup.get_text()
        for cond in ['良', '稍重', '重', '不良']:
            if cond in body:
                race_meta['condition'] = cond
                break
    except Exception:
        pass

    # 多くの netkeiba 結果ページは class 'race_table_01' のテーブルを利用
    tables = soup.find_all('table')
    candidate_rows = []
    header_names = []
    for tbl in tables:
        # 判定: ヘッダーに '着順' 等があれば対象
        header = tbl.find('th')
        if header and any(x in header.get_text() for x in ['着順', '馬番', '枠']):
            # try to capture header names to find odds/horse columns
            header_row = None
            for tr in tbl.find_all('tr'):
                ths = tr.find_all('th')
                if ths:
                    header_row = tr
                    break
            if header_row:
                header_names = [th.get_text(strip=True) for th in header_row.find_all('th')]
            # collect rows
            for tr in tbl.find_all('tr'):
                tds = tr.find_all(['td', 'th'])
                if len(tds) >= 3:
                    candidate_rows.append(tr)
            break

    # fallback: find links to horse pages and their containing rows
    if not candidate_rows:
        for a in soup.find_all('a', href=True):
            if '/horse/' in a['href'] or 'horse' in a['href']:
                tr = a.find_parent('tr')
                if tr:
                    candidate_rows.append(tr)

    for tr in candidate_rows:
        try:
            cols = tr.find_all('td')
            if not cols:
                continue
            text = [c.get_text(strip=True) for c in cols]
            # prefer explicit horse link
            horse_url = None
            horse_name = None
            ha = tr.find('a', href=lambda h: h and '/horse/' in h)
            if ha:
                horse_url = ha['href']
                horse_name = ha.get_text(strip=True)
            else:
                # fallback: if header names indicate a horse name column, use its cell
                horse_col_idx = None
                for idx, hn in enumerate(header_names):
                    if '馬名' in hn or hn.strip() in ('馬', '馬名'):
                        horse_col_idx = idx
                        break
                if horse_col_idx is not None and horse_col_idx < len(cols):
                    horse_name = cols[horse_col_idx].get_text(strip=True)

            # heuristics for rank/odds/jockey
            rank = None
            jockey = None
            weight = None
            odds = None
            horse_no = None

            # try find numeric rank in first cols
            for c in cols[:3]:
                txt = c.get_text(strip=True)
                if txt.isdigit():
                    # use first digit as rank if small
                    val = int(txt)
                    if 1 <= val <= 20 and rank is None:
                        rank = val
            # jockey detection: prefer explicit jockey link if present
            jockey = None
            ja = tr.find('a', href=lambda h: h and '/jockey/' in h)
            if ja:
                jockey = ja.get('title') or ja.get_text(strip=True)
            else:
                for c in cols:
                    txt = c.get_text(strip=True)
                    if '騎' in txt or '騎手' in txt or '騎手名' in txt:
                        jockey = txt
            # odds detection: prefer column by header name if available
            odds_col_idx = None
            for idx, hn in enumerate(header_names):
                if any(k in hn for k in ['単勝', 'オッズ', '単勝オッズ']):
                    odds_col_idx = idx
                    break
            if odds_col_idx is not None and odds_col_idx < len(cols):
                txt = cols[odds_col_idx].get_text(strip=True)
                try:
                    # remove commas and unexpected chars
                    txt_norm = txt.replace(',', '').strip()
                    odds_val = float(re.search(r"[0-9]+\.?[0-9]*", txt_norm).group(0)) if re.search(r"[0-9]+\.?[0-9]*", txt_norm) else None
                    if odds_val and 0 < odds_val < 1000:
                        odds = odds_val
                except Exception:
                    odds = None
            if odds is None:
                # fallback: scan cells for a reasonable numeric odds value
                for c in cols:
                    txt = c.get_text(strip=True)
                    m = re.search(r"(\d+\.\d+|\d+\.?)", txt)
                    if m and odds is None:
                        try:
                            val = float(m.group(1))
                            # skip year-like large values
                            if 0 < val < 1000:
                                odds = val
                        except Exception:
                            pass

            results.append({
                'horse_name': horse_name,
                'horse_url': horse_url,
                'rank': rank,
                'jockey': jockey,
                'weight': weight,
                'odds': odds,
                'horse_no': horse_no,
            })
        except Exception:
            continue

    return results, race_meta


def parse_horse_page_for_pedigree(html: str) -> Dict[str, Optional[str]]:
    """馬ページ HTML から '父' と '母父' を抽出する。"""
    soup = BeautifulSoup(html, 'html.parser')
    father = None
    damsire = None

    # 1) テキストベースで '父' / '母父' のパターンを探す
    alltext = soup.get_text(separator='\n')
    stopwords = set(['血統', '産駒', '血', '掲', '写', '血統', '血統表', '父', '母'])
    if not father:
        m = re.search(r'父[:：]?\s*([^\n\[\(]{1,80})', alltext)
        if m:
            cand = m.group(1).strip()
            # require candidate contains at least one Japanese kana/kanji or alphabetic char and sensible length
            if re.search(r'[\u3040-\u30ff\u4e00-\u9fafA-Za-z]', cand) and len(cand) > 1 and cand not in stopwords:
                father = cand
    if not damsire:
        m = re.search(r'母父[:：]?\s*([^\n\[\(]{1,80})', alltext)
        if m:
            cand = m.group(1).strip()
            if re.search(r'[\u3040-\u30ff\u4e00-\u9fafA-Za-z]', cand) and len(cand) > 1 and cand not in stopwords:
                damsire = cand

    # 2) タグ近傍検索: テキスト『父』を含むタグの近くにある a タグを探す
    if (not father) or (not damsire):
        stopwords = set(['血統', '産駒', '血', '掲', '写', '血統', '血統表', '父', '母'])
        for tag in soup.find_all():
            txt = tag.get_text(strip=True)
            if not txt:
                continue
            # exact-match for '父' / '母父' to avoid matching '叔父母' etc.
            if txt in ('父', '父:') and (not father):
                a = tag.find_next('a')
                if a:
                    name = a.get_text(strip=True)
                    if name and name not in stopwords and len(name) > 1:
                        father = name
            if txt in ('母父', '母父:') and (not damsire):
                a = tag.find_next('a')
                if a:
                    name = a.get_text(strip=True)
                    if name and name not in stopwords and len(name) > 1:
                        damsire = name
            if father and damsire:
                break

    # 3) フォールバック: 血統テーブル (class に 'blood_table') があれば、表中の horse リンクを順に取り、最初の2つを父/母父に使う
    if (not father) or (not damsire):
        tbl = None
        for t in soup.find_all('table'):
            cls = ' '.join(t.get('class') or [])
            if 'blood_table' in cls or 'blood_table detail' in cls:
                tbl = t
                break
        if tbl:
            raw_anchors = [a for a in tbl.find_all('a', href=True) if '/horse/' in a['href']]
            # filter out non-name anchors like '血統','産駒','[血]' etc.
            stopwords = set(['血統', '産駒', '血', '掲', '写', '血統', '血統表', '父', '母'])
            anchors = []
            for a in raw_anchors:
                txt = a.get_text(strip=True)
                if not txt:
                    continue
                # remove bracket markers
                t = txt.replace('[','').replace(']','').strip()
                if t in stopwords:
                    continue
                if len(t) <= 1:
                    continue
                if t.isdigit():
                    continue
                anchors.append(t)
            # dedupe while preserving order
            seen = []
            for a in anchors:
                if a and a not in seen:
                    seen.append(a)
            if seen:
                # common layout heuristic: seen[1] is often 父 (sire), seen[2] is 母父 (damsire)
                if not father and len(seen) > 1:
                    father = seen[1]
                if not damsire and len(seen) > 2:
                    damsire = seen[2]
                # fallback to earlier behavior if not enough anchors
                if not father and len(seen) > 0:
                    father = seen[0]
                if not damsire and len(seen) > 1:
                    damsire = seen[1]

    # 最後に normalize: None または空文字は None に
    if father:
        father = father.strip() or None
    if damsire:
        damsire = damsire.strip() or None

    return {'father': father, 'damsire': damsire}


def scrape_dates(start_date: datetime.date, end_date: datetime.date, out_parquet: str,
                 sleep: float = 1.0, max_races: Optional[int] = None):
    sess = requests.Session()
    sess.headers.update({'User-Agent': USER_AGENT})
    proc = HorseDataProcessor()

    rows = []
    race_count = 0

    for d in daterange(start_date, end_date):
        # netkeiba の日付別レース一覧ページ (db.netkeiba の日別一覧を優先)
        date_str = d.strftime('%Y%m%d')
        list_url_db = f'https://db.netkeiba.com/race/list/{date_str}/'
        list_url_race = f'https://race.netkeiba.com/?pid=race_list&date={date_str}'
        # まず db の日別リストを試し、なければ race.netkeiba のリストを使う
        html = fetch_url(sess, list_url_db)
        if not html:
            html = fetch_url(sess, list_url_race)
        if not html:
            time.sleep(sleep)
            continue
        links = find_race_links_on_date(html)
        # normalize links to absolute
        full_links = []
        for href in links:
            if href.startswith('http'):
                full_links.append(href)
            else:
                # '/race/...' のような db.netkeiba の相対パスなら db をプレフィックス
                if href.startswith('/race/') or href.startswith('race/'):
                    full_links.append('https://db.netkeiba.com' + href)
                else:
                    full_links.append('https://race.netkeiba.com' + href)

        for race_url in full_links:
            # limit
            if max_races and race_count >= max_races:
                break
            race_html = fetch_url(sess, race_url)
            if not race_html:
                time.sleep(sleep)
                continue
            parsed = parse_race_result(race_html, race_url)
            if isinstance(parsed, tuple):
                race_results, race_meta = parsed
            else:
                race_results = parsed
                race_meta = {'race_class': None, 'surface': None, 'distance': None, 'course_direction': None, 'condition': None}
            for hr in race_results:
                horse_page_url = hr.get('horse_url')
                father = None
                damsire = None
                age_sex = None
                rest_days = None
                recent_ranks = None
                weight_carried = None
                trainer = None
                jockey_page = None
                jockey_stats = {'win_rate': None, 'place_rate': None}
                jockey_horse_past_count = None
                if horse_page_url:
                    # Prefer the pedigree (血統) page under db.netkeiba if possible
                    if horse_page_url.startswith('http'):
                        raw = horse_page_url
                    else:
                        raw = horse_page_url
                    if raw.startswith('/'):
                        # convert '/horse/12345/' -> '/horse/ped/12345/'
                        if raw.startswith('/horse/'):
                            ped_path = raw.replace('/horse/', '/horse/ped/')
                        else:
                            ped_path = raw
                        hp_url = 'https://db.netkeiba.com' + ped_path
                    else:
                        # absolute path, prefer db domain
                        if '/horse/' in raw:
                            hp_url = raw.replace('/horse/', '/horse/ped/')
                        else:
                            hp_url = raw
                    horse_html = fetch_url(sess, hp_url)
                    if horse_html:
                        pedigree = parse_horse_page_for_pedigree(horse_html)
                        father = pedigree.get('father')
                        damsire = pedigree.get('damsire')
                        # try to extract age/sex and recent results from horse page
                        try:
                            txt = BeautifulSoup(horse_html, 'html.parser').get_text('\n')
                            # age/sex: 牡4 牝3 など
                            m = re.search(r'(牡|牝|セ|騙)\s*(\d{1,2})', txt)
                            if m:
                                age_sex = m.group(1) + str(m.group(2))
                            # recent race dates: find YYYY-MM-DD or YYYY/MM/DD
                            dates = re.findall(r'20\d{2}[/-]\d{2}[/-]\d{2}', txt)
                            if dates:
                                recent_dates = [d.replace('/', '-') for d in dates]
                            else:
                                recent_dates = []
                            # recent ranks: try to find small tables with '着' etc. Fallback: None
                            ranks = re.findall(r'\b(\d{1,2})着\b', txt)
                            if ranks:
                                recent_ranks = ranks[:5]
                        except Exception:
                            pass
                        time.sleep(sleep)

                father_group = proc.map_lineage(father)
                damsire_group = proc.map_lineage(damsire)

                row = {
                    'race_url': race_url,
                    'race_date': d.isoformat(),
                    'horse_name': hr.get('horse_name'),
                    'rank': hr.get('rank'),
                    'jockey': hr.get('jockey'),
                    'odds': hr.get('odds'),
                    'age_sex': age_sex,
                    'rest_days': rest_days,
                    'recent_ranks': recent_ranks,
                    'weight_carried': weight_carried,
                    'trainer': trainer,
                    'jockey_win_rate': jockey_stats.get('win_rate'),
                    'jockey_place_rate': jockey_stats.get('place_rate'),
                    'jockey_horse_past_count': jockey_horse_past_count,
                    'father': father,
                    'damsire': damsire,
                    'father_group': father_group,
                    'damsire_group': damsire_group,
                    'race_class': race_meta.get('race_class'),
                    'surface': race_meta.get('surface'),
                    'distance': race_meta.get('distance'),
                    'course_direction': race_meta.get('course_direction'),
                    'condition': race_meta.get('condition'),
                }
                rows.append(row)

            race_count += 1
            time.sleep(sleep)

        # flush intermediate results after each date so supervisor/predictor can run incrementally
        try:
            df_partial = pd.DataFrame(rows)
            df_partial.to_parquet(out_parquet, engine='pyarrow', index=False)
        except Exception:
            pass

        if max_races and race_count >= max_races:
            break

    df = pd.DataFrame(rows)
    # 保存
    df.to_parquet(out_parquet, engine='pyarrow', index=False)
    print(f'Saved {len(df)} rows to {out_parquet}')


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--start-date', required=True, help='開始日 YYYY-MM-DD')
    p.add_argument('--end-date', required=True, help='終了日 YYYY-MM-DD')
    p.add_argument('--out', required=True, help='出力Parquetファイルパス')
    p.add_argument('--sleep', type=float, default=1.0, help='リクエスト間のsleep秒')
    p.add_argument('--max-races', type=int, default=None, help='最大取得レース数（テスト用）')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    sd = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').date()
    ed = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()
    scrape_dates(sd, ed, args.out, sleep=args.sleep, max_races=args.max_races)
