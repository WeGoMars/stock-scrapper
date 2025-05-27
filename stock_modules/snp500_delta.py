import yfinance as yf
import datetime

# === 날짜 설정 ===
today = datetime.date.today()
past_1m = today - datetime.timedelta(days=30)
past_3m = today - datetime.timedelta(days=90)
past_6m = today - datetime.timedelta(days=180)

# === S&P500 지수 불러오기 ===
ticker = "^GSPC"  # Yahoo Finance 심볼
sp500 = yf.Ticker(ticker)

# === 과거 6개월 데이터 불러오기 ===
hist = sp500.history(start=past_6m, end=today)

# === 날짜별 종가 추출 ===
hist_close = hist["Close"]

# === 가장 가까운 날짜 찾기 함수 ===
def get_closest_price(target_date: datetime.date) -> float:
    # 날짜 포맷 yyyy-mm-dd 와 가장 가까운 index를 찾음
    sorted_dates = hist_close.index.sort_values()
    for offset in range(7):
        for delta in [offset, -offset]:
            check = target_date + datetime.timedelta(days=delta)
            try:
                return hist_close.loc[str(check)]
            except KeyError:
                continue
    return None

# === 가격 추출 ===
latest_price = get_closest_price(today)
price_1m = get_closest_price(past_1m)
price_3m = get_closest_price(past_3m)
price_6m = get_closest_price(past_6m)

# === 수익률 계산 ===
def calc_change(current, past):
    if current is None or past is None:
        return None
    return round((current - past) / past * 100, 2)

# === 출력 ===
print("📊 S&P500 수익률 변화율 계산:")
print(f"현재: {latest_price}")
print(f"1개월 전: {price_1m} → {calc_change(latest_price, price_1m)}%")
print(f"3개월 전: {price_3m} → {calc_change(latest_price, price_3m)}%")
print(f"6개월 전: {price_6m} → {calc_change(latest_price, price_6m)}%")