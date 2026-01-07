from data_fetcher import fetch_and_save_tickers
fetch_and_save_tickers(start=1000, end=9999, batch_size=100, period='1y', interval='1d', out_dir='data', retry_count=2, sleep_between_batches=2.0, allow_excluded=False, verbose=True)
