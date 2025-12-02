import pandas as pd

def moving_average(series, window):
    return series.rolling(window=window).mean()

def calculate_ma(series, window):
    return series.rolling(window=window).mean()
