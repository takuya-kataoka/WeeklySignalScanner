#!/usr/bin/env python3
"""
Increment VERSION file by 0.01 (e.g. 1.00 -> 1.01).
Run this before committing when you want to bump the app version.
"""
from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent
version_file = repo_root / 'VERSION'

def read_version():
    if not version_file.exists():
        return 1.00
    try:
        txt = version_file.read_text(encoding='utf-8').strip()
        return float(txt)
    except Exception:
        return 1.00

def write_version(v: float):
    version_file.write_text(f"{v:.2f}\n", encoding='utf-8')

def bump():
    v = read_version()
    v += 0.01
    # round to 2 decimal places to avoid float printing issues
    v = round(v + 1e-8, 2)
    write_version(v)
    print(f"New version: {v:.2f}")

if __name__ == '__main__':
    bump()
