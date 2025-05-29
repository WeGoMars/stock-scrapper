import yfinance as yf
from datetime import date, timedelta
from utils.dates import get_last_business_day_of_month
import pandas as pd

def calc_change(current, past):
    if current is None or past is None:
        return None
    return round((current - past) / past * 100, 2)

def get_closest_price(hist: pd.Series, target: date) -> float:
    for offset in range(7):
        for delta in [offset, -offset]:
            check = target + timedelta(days=delta)
            try:
                return hist.loc[str(check)]
            except KeyError:
                continue
    return None

def fetch_snp500_multi_returns(base_date: date, max_months: int = 12) -> list[dict]:
    start_date = (base_date.replace(day=1) - timedelta(days=32 * max_months)).replace(day=1)
    ticker = yf.Ticker("^GSPC")
    hist = ticker.history(start=start_date, end=base_date)
    hist_close = hist["Close"]

    result = []
    for i in range(1, max_months + 1):
        offset_month = base_date.replace(day=1) - timedelta(days=32 * i)
        price_then = get_closest_price(hist_close, get_last_business_day_of_month(offset_month))
        price_now = get_closest_price(hist_close, get_last_business_day_of_month(base_date))

        pct = calc_change(price_now, price_then)
        if pct is not None:
            result.append({
                "name": f"SNP500_{i}M_RETURN",
                "timestamp": base_date,
                "value": pct
            })

    return result
