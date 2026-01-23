#!/usr/bin/env python3
"""
jra_pdf_fetcher.py

Download JRA PDF files given either a list of URLs or a URL template and date range.

Usage:
  # Download from a list of URLs in pdf_urls.txt
  python3 scripts/jra_pdf_fetcher.py --urls-file pdf_urls.txt --out-dir pdfs/

  # Or provide a URL template (use {date} as YYYYMMDD) and date range
  python3 scripts/jra_pdf_fetcher.py --template "https://example.org/pdf/{date}.pdf" --start 2023-01-01 --end 2023-01-31 --out-dir pdfs/

Notes:
- This script does not attempt site-specific discovery. Provide a template or explicit URLs.
"""
import argparse
import os
import sys
import requests
from datetime import datetime, timedelta


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def download_url(url, out_path, timeout=30):
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        with open(out_path, 'wb') as f:
            f.write(r.content)
        return True, None
    except Exception as e:
        return False, str(e)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--urls-file', help='File with one PDF URL per line')
    p.add_argument('--template', help='URL template containing {date} as YYYYMMDD')
    p.add_argument('--start', help='Start date YYYY-MM-DD')
    p.add_argument('--end', help='End date YYYY-MM-DD')
    p.add_argument('--out-dir', default='pdfs', help='Output directory for downloaded PDFs')
    args = p.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    targets = []
    if args.urls_file:
        with open(args.urls_file) as f:
            for line in f:
                u = line.strip()
                if u:
                    targets.append((u, None))

    if args.template and args.start and args.end:
        sd = datetime.strptime(args.start, '%Y-%m-%d').date()
        ed = datetime.strptime(args.end, '%Y-%m-%d').date()
        for d in daterange(sd, ed):
            date_str = d.strftime('%Y%m%d')
            url = args.template.format(date=date_str)
            targets.append((url, d.isoformat()))

    if not targets:
        print('No targets. Provide --urls-file or --template with --start/--end', file=sys.stderr)
        sys.exit(2)

    results = []
    for url, date_iso in targets:
        fn = os.path.basename(url.split('?')[0])
        if not fn.lower().endswith('.pdf'):
            fn = f'{date_iso or "unknown"}_{fn}.pdf'
        out_path = os.path.join(args.out_dir, fn)
        ok, err = download_url(url, out_path)
        results.append((url, out_path, ok, err))
        print(url, '->', out_path, 'ok' if ok else f'ERR: {err}')

    # summary
    succ = sum(1 for r in results if r[2])
    fail = len(results) - succ
    print(f'Download finished: {succ} success, {fail} failed')


if __name__ == '__main__':
    main()
