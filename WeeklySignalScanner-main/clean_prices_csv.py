import re
import os

in_path = 'outputs/results/jp_all_ma52_engulfing_with_prices_2025-12-12.csv'
out_path = 'outputs/results/jp_all_ma52_engulfing_with_prices_2025-12-12.cleaned.csv'

if not os.path.exists(in_path):
    raise SystemExit(f"入力ファイルが見つかりません: {in_path}")

num_re = re.compile(r"([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)")

with open(in_path, 'r', encoding='utf-8') as f_in, open(out_path, 'w', encoding='utf-8') as f_out:
    header = f_in.readline()
    if not header:
        raise SystemExit('空のファイル')
    # write normalized header
    f_out.write('ticker,current_price\n')
    for line in f_in:
        line = line.strip()
        if not line:
            continue
        parts = line.split(',', 1)
        if len(parts) == 1:
            ticker = parts[0].strip()
            price = ''
        else:
            ticker = parts[0].strip()
            rest = parts[1].strip()
            # try to extract first numeric substring
            m = num_re.search(rest)
            price = m.group(1) if m else ''
        f_out.write(f"{ticker},{price}\n")

print(f"Cleaned file written to: {out_path}")
