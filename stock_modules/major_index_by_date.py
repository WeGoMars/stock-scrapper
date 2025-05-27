import requests
import os
from dotenv import load_dotenv
from datetime import datetime

# 환경변수에서 API 키 불러오기
load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

# ===== 기준일 설정 =====
target_date_str = "2023-12-31"
target_date = datetime.strptime(target_date_str, "%Y-%m-%d")

# ===== 종목 및 URL 구성 =====
symbol = "AAPL"
base_url = "https://financialmodelingprep.com/api/v3"

profile_url = f"{base_url}/profile/{symbol}?apikey={API_KEY}"
ratios_url = f"{base_url}/ratios-ttm/{symbol}?apikey={API_KEY}"
market_cap_url = f"{base_url}/market-capitalization/{symbol}?apikey={API_KEY}"
income_url = f"{base_url}/income-statement/{symbol}?limit=100&apikey={API_KEY}"
balance_url = f"{base_url}/balance-sheet-statement/{symbol}?limit=100&apikey={API_KEY}"

# ===== 공통 fetch 함수 =====
def fetch_json(url):
    res = requests.get(url)
    if res.ok and res.json():
        return res.json()
    return []

def find_latest_before(data_list, date_field="date"):
    for entry in sorted(data_list, key=lambda x: x[date_field], reverse=True):
        try:
            entry_date = datetime.strptime(entry[date_field], "%Y-%m-%d")
            if entry_date <= target_date:
                return entry
        except:
            continue
    return {}

# ===== 데이터 불러오기 =====
profile = fetch_json(profile_url)[0]
ratios = fetch_json(ratios_url)[0]
market_cap = fetch_json(market_cap_url)[0]
income_list = fetch_json(income_url)
balance_list = fetch_json(balance_url)

income = find_latest_before(income_list)
balance = find_latest_before(balance_list)

# ===== EPS 계산 =====
try:
    eps = round(float(income["netIncome"]) / float(income["weightedAverageShsOut"]), 2)
except:
    eps = "N/A"

# ===== BPS 계산 =====
try:
    equity = float(balance["totalStockholdersEquity"])
    price = float(profile["price"])
    market_cap_val = float(market_cap["marketCap"])
    shares = market_cap_val / price
    bps = round(equity / shares, 2)
except:
    bps = "N/A"

# ===== 배당수익률 =====
try:
    last_div = float(profile["lastDiv"])
    price = float(profile["price"])
    dividend_yield = round(last_div / price * 100, 2)
except:
    dividend_yield = "N/A"

# ===== 유동비율 (Current Ratio) =====
try:
    current_ratio = round(
        float(balance["totalCurrentAssets"]) / float(balance["totalCurrentLiabilities"]), 2
    )
except:
    current_ratio = "N/A"

# ===== 부채비율 (Debt Ratio) =====
try:
    debt_ratio = round(
        float(balance["totalLiabilities"]) / float(balance["totalStockholdersEquity"]), 2
    )
except:
    debt_ratio = "N/A"

# ===== 결과 출력 =====
print(f"\n✅ 기준일 기준 [{symbol}] 주요 지표 수집 결과 (기준일: {target_date_str})")
print(f"산업 (Industry): {profile.get('industry', 'N/A')}")
print(f"섹터 (Sector): {profile.get('sector', 'N/A')}")
print(f"시가총액 (Market Cap): {market_cap.get('marketCap', 'N/A')}")
print(f"ROE: {ratios.get('returnOnEquityTTM', 'N/A')}")
print(f"EPS (계산): {eps}")
print(f"BPS (계산): {bps}")
print(f"Beta: {profile.get('beta', 'N/A')}")
print(f"배당수익률 (Dividend Yield): {dividend_yield}%")
print(f"유동비율 (Current Ratio): {current_ratio}")
print(f"부채비율 (Debt Ratio): {debt_ratio}")