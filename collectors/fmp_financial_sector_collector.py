# collectors/fmp_financial_collector.py

import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from itertools import cycle
from typing import List, Dict
from datetime import datetime
from time import sleep


load_dotenv()
API_KEYS = os.getenv("FMP_API_KEYS", "").split(",")
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
api_key_cycle = cycle(API_KEYS)

def get_next_api_key():
    return next(api_key_cycle)

def fetch_json(url):
    try:
        response = requests.get(url, timeout=10)
        if response.ok:
            return response.json()
    except Exception as e:
        print(f"❌ 요청 실패: {url} → {e}")
    return None

def find_latest_before(data_list, target_date, date_field="date"):
    for entry in sorted(data_list, key=lambda x: x[date_field], reverse=True):
        try:
            entry_date = datetime.strptime(entry[date_field], "%Y-%m-%d")
            if entry_date <= target_date:
                return entry
        except:
            continue
    return {}

class FinancialDataIncompleteError(Exception):
    def __init__(self, symbol: str, missing_fields: list[str]):
        message = f"{symbol}의 재무 데이터 수집 실패 - 누락된 필드: {', '.join(missing_fields)}"
        super().__init__(message)
        self.symbol = symbol
        self.missing_fields = missing_fields


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        raise ZeroDivisionError("0으로 나눌 수 없습니다.")
    return round(numerator / denominator, 2)

def collect_fmp_stock_financials(symbol: str, target_date: datetime) -> dict:
    print(f"📦 수집 중: {symbol} 재무정보")
    api_key = get_next_api_key()

    urls = {
        "profile": f"{FMP_BASE_URL}/profile/{symbol}?apikey={api_key}",
        "ratios": f"{FMP_BASE_URL}/ratios-ttm/{symbol}?apikey={api_key}",
        "market_cap": f"{FMP_BASE_URL}/market-capitalization/{symbol}?apikey={api_key}",
        "income": f"{FMP_BASE_URL}/income-statement/{symbol}?limit=100&apikey={api_key}",
        "balance": f"{FMP_BASE_URL}/balance-sheet-statement/{symbol}?limit=100&apikey={api_key}",
    }

    profile = fetch_json(urls["profile"])
    ratios = fetch_json(urls["ratios"])
    market_cap = fetch_json(urls["market_cap"])
    income_list = fetch_json(urls["income"])
    balance_list = fetch_json(urls["balance"])

    if not (profile and ratios and market_cap and income_list and balance_list):
        raise FinancialDataIncompleteError(symbol, ["API 응답 누락"])

    profile = profile[0]
    ratios = ratios[0]
    market_cap = market_cap[0]
    income = find_latest_before(income_list, target_date)
    balance = find_latest_before(balance_list, target_date)

    try:
        eps = safe_divide(float(income["netIncome"]), float(income["weightedAverageShsOut"]))
        equity = float(balance["totalStockholdersEquity"])
        price = float(profile["price"])
        shares = float(market_cap["marketCap"]) / price
        bps = safe_divide(equity, shares)
        dividend_yield = safe_divide(float(profile["lastDiv"]), price) * 100
        current_ratio = safe_divide(float(balance["totalCurrentAssets"]), float(balance["totalCurrentLiabilities"]))
        debt_ratio = safe_divide(float(balance["totalLiabilities"]), equity)
    except (KeyError, ValueError, ZeroDivisionError) as e:
        raise FinancialDataIncompleteError(symbol, [str(e)])

    result = {
        "symbol": symbol,
        "targetDate": target_date.strftime("%Y-%m-%d"),
        "industry": profile.get("industry"),
        "sector": profile.get("sector"),
        "marketCap": market_cap.get("marketCap"),
        "roe": ratios.get("returnOnEquityTTM"),
        "eps": eps,
        "bps": bps,
        "beta": profile.get("beta"),
        "dividendYield": dividend_yield,
        "currentRatio": current_ratio,
        "debtRatio": debt_ratio
    }

    # 누락된 필드 체크
    missing_fields = [k for k, v in result.items() if v is None]
    if missing_fields:
        raise FinancialDataIncompleteError(symbol, missing_fields)

    print(f"✅ {symbol} 수집 완료 → {result}")
    return result









def fetch_sector_performance_from_fmp(max_retries: int = 3) -> List[Dict]:
    """
    FMP API에서 섹터별 수익률을 수집합니다.
    Returns: [{"sector": "Technology", "return": 1.23}, ...]
    """
    for _ in range(max_retries):
        api_key = get_next_api_key()
        url = f"{FMP_BASE_URL}/sectors-performance?apikey={api_key}"

        try:
            response = requests.get(url, timeout=10)
            
            print(f"📡 요청 URL: {url}")
            print(f"📦 응답 상태코드: {response.status_code}")
            print("📄 응답 본문:")
            print(response.text)  # 전체 본문 출력
            
            if response.status_code != 200:
                print(f"❌ 요청 실패: {response.status_code} - {response.text}")
                continue

            data = response.json()

            if not isinstance(data, list):
                print("⚠️ 응답 형식이 예상과 다릅니다.")
                continue

            result = []
            for item in data:
                sector = item.get("sector")
                change_str = item.get("changesPercentage")

                if not sector or not change_str:
                    continue

                try:
                    change = float(change_str.replace('%', ''))
                    result.append({"sector": sector, "return": change})
                except ValueError:
                    continue

            return result

        except Exception as e:
            print(f"⚠️ 예외 발생: {e}")

    print("❌ 모든 키에서 요청 실패")
    return []
