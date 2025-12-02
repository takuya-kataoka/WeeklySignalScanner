from screener import scan_stocks

def main():
    print("=== 日本株スクリーナー ===")
    results = scan_stocks()

    if results:
        print("\n🔔 条件を満たした銘柄 🔔")
        for t in results:
            print(f"- {t}")
    else:
        print("\n該当銘柄なしでした…")

if __name__ == "__main__":
    main()

