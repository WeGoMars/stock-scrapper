import yfinance as yf
from datetime import date, timedelta
from utils.dates import get_last_business_day_of_month
import pandas as pd

def calc_change(current, past):
    if current is None or past is None:
        return None
    return round((current - past) / past * 100, 2)

def get_closest_price(hist: pd.Series, target: date) -> float:
    """
    주어진 날짜 기준 ±7일 이내의 가장 가까운 종가를 반환.
    hist.index는 datetime.date 형식이어야 함.
    """
    for offset in range(7):
        for delta in [offset, -offset]:
            check = target + timedelta(days=delta)
            if check in hist.index:
                return hist.loc[check]
    print(f"❌ get_closest_price 실패: {target} ±7일 내 데이터 없음")
    return None


def fetch_snp500_multi_returns(base_date: date, max_months: int = 12) -> list[dict]:
    from utils.dates import get_last_business_day_of_month

    start_date = (base_date.replace(day=1) - timedelta(days=32 * max_months)).replace(day=1)
    ticker = yf.Ticker("^GSPC")
    hist = ticker.history(start=start_date, end=base_date)
    hist_close = hist["Close"]
    hist_close.index = hist_close.index.date  # 날짜만 남김

    result = []

    for i in range(1, max_months + 1):
        offset_month = base_date.replace(day=1) - timedelta(days=32 * i)
        target_then = get_last_business_day_of_month(offset_month)
        target_now = get_last_business_day_of_month(base_date)

        price_then = get_closest_price(hist_close, target_then)
        price_now_date = max(hist_close.index)  # 실제 존재하는 마지막 날짜
        price_now = hist_close.loc[price_now_date]

        print(f"[{i}M] 기준일: {target_then} → {price_now_date}")
        print(f"   ↳ 과거가: {price_then}, 현재가: {price_now}")


        pct = calc_change(price_now, price_then)
        if pct is not None:
            result.append({
                "name": f"SNP500_{i}M_RETURN",
                "timestamp": base_date,
                "value": pct
            })
        else:
            print(f"❌ 수익률 계산 불가 (None 포함): {price_then=} {price_now=}")

    return result
