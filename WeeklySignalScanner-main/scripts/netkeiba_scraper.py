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


USER_AGENT = 'Mozilla/5.0 (compatible; NetKeibaScraper/1.0; +https://example.com)'


def daterange(start_date: datetime.date, end_date: datetime.date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def fetch_url(session: requests.Session, url: str, timeout: int = 20) -> Optional[str]:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def find_race_links_on_date(html: str) -> List[str]:
    """日付ページの HTML からレース結果ページへのリンクを抽出する（汎用的）。"""
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # 一般的に race/result や /race/ のパスを含むリンクを拾う
        if ('result' in href or '/race/' in href) and ('horse' not in href):
            # フルURL化は呼び出し側で
            links.append(href)
    # uniq
    return list(dict.fromkeys(links))


def parse_race_result(html: str, base_url: str) -> List[Dict]:
    """レース結果ページ HTML を解析して馬ごとの基本情報と馬ページURLを返す。

    返却する dict 要素: horse_name, horse_url, rank, jockey, weight, odds (可能なら), horse_no
    """
    soup = BeautifulSoup(html, 'html.parser')
    results = []

    # 多くの netkeiba 結果ページは class 'race_table_01' のテーブルを利用
    tables = soup.find_all('table')
    candidate_rows = []
    for tbl in tables:
        # 判定: ヘッダーに '着順' 等があれば対象
        header = tbl.find('th')
        if header and any(x in header.get_text() for x in ['着順', '馬番', '枠']):
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
            # attempt to find horse link
            a = tr.find('a', href=True)
            horse_url = None
            horse_name = None
            if a and ('horse' in a['href'] or '/horse/' in a['href']):
                horse_url = a['href']
                horse_name = a.get_text(strip=True)

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
            # jockey detection: contains '騎' or typical name
            for c in cols:
                txt = c.get_text(strip=True)
                if '騎' in txt or '騎手' in txt:
                    jockey = txt
            # odds detection: look for decimal or x.x
            for c in cols:
                txt = c.get_text(strip=True)
                m = re.search(r"(\d+\.\d+|\d+\.?)", txt)
                if m and odds is None:
                    try:
                        odds = float(m.group(1))
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

    return results


def parse_horse_page_for_pedigree(html: str) -> Dict[str, Optional[str]]:
    """馬ページ HTML から '父' と '母父' を抽出する。"""
    soup = BeautifulSoup(html, 'html.parser')
    father = None
    damsire = None

    # 試作: '父' '母父' を含む表の th を探す
    for th in soup.find_all(['th', 'td']):
        txt = th.get_text(strip=True)
        if txt in ['父', '父:']:
            td = th.find_next_sibling('td')
            if td:
                father = td.get_text(strip=True).split()[0]
        if txt in ['母父', '母父:']:
            td = th.find_next_sibling('td')
            if td:
                damsire = td.get_text(strip=True).split()[0]

    # fallback: look for patterns like '父：' or '母父：' in text
    alltext = soup.get_text(separator='\n')
    if not father:
        m = re.search(r'父[:：]\s*([\w\u3000-]+)', alltext)
        if m:
            father = m.group(1).strip()
    if not damsire:
        m = re.search(r'母父[:：]\s*([\w\u3000-]+)', alltext)
        if m:
            damsire = m.group(1).strip()

    return {'father': father, 'damsire': damsire}


def scrape_dates(start_date: datetime.date, end_date: datetime.date, out_parquet: str,
                 sleep: float = 1.0, max_races: Optional[int] = None):
    sess = requests.Session()
    sess.headers.update({'User-Agent': USER_AGENT})
    proc = HorseDataProcessor()

    rows = []
    race_count = 0

    for d in daterange(start_date, end_date):
        # netkeiba の日付別レース一覧ページ (汎用推定 URL)。必要に応じて調整してください。
        date_str = d.strftime('%Y%m%d')
        list_url = f'https://race.netkeiba.com/?pid=race_list&date={date_str}'
        html = fetch_url(sess, list_url)
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
                full_links.append('https://race.netkeiba.com' + href)

        for race_url in full_links:
            # limit
            if max_races and race_count >= max_races:
                break
            race_html = fetch_url(sess, race_url)
            if not race_html:
                time.sleep(sleep)
                continue
            race_results = parse_race_result(race_html, race_url)
            for hr in race_results:
                horse_page_url = hr.get('horse_url')
                father = None
                damsire = None
                if horse_page_url:
                    if horse_page_url.startswith('http'):
                        hp_url = horse_page_url
                    else:
                        hp_url = 'https://db.netkeiba.com' + horse_page_url if horse_page_url.startswith('/') else 'https://race.netkeiba.com' + horse_page_url
                    horse_html = fetch_url(sess, hp_url)
                    if horse_html:
                        pedigree = parse_horse_page_for_pedigree(horse_html)
                        father = pedigree.get('father')
                        damsire = pedigree.get('damsire')
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
                    'father': father,
                    'damsire': damsire,
                    'father_group': father_group,
                    'damsire_group': damsire_group,
                }
                rows.append(row)

            race_count += 1
            time.sleep(sleep)

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
