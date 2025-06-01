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

def normalize_fmp_symbol(symbol: str) -> str:
    """
    FMP API에 사용할 수 있도록 심볼에서 '.'를 '-'로 변환합니다.
    예: 'BF.B' → 'BF-B'
    """
    return symbol.replace('.', '-')


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        raise ZeroDivisionError("0으로 나눌 수 없습니다.")
    return round(numerator / denominator, 2)


def collect_fmp_stock_financials(symbol: str, target_date: datetime) -> dict:
    print(f"📦 {symbol} 재무지표 수집 시작")
    api_key = get_next_api_key()
    normalized = normalize_fmp_symbol(symbol)

    url_metrics = f"{FMP_BASE_URL}/key-metrics-ttm/{normalized}?apikey={api_key}"
    url_profile = f"{FMP_BASE_URL}/profile/{normalized}?apikey={api_key}"

    metrics_data = fetch_json(url_metrics)
    profile_data = fetch_json(url_profile)
    
    print(f"📡 요청 URL (metrics): {url_metrics}")
    print("📄 응답 본문 (metrics):")
    print(metrics_data)

    print(f"📡 요청 URL (profile): {url_profile}")
    print("📄 응답 본문 (profile):")
    print(profile_data)

    if not metrics_data or not profile_data:
        raise FinancialDataIncompleteError(symbol, ["API 응답 누락"])

    metrics = metrics_data[0]
    profile = profile_data[0]

    result = {
        "symbol": symbol,
        "targetDate": target_date.strftime("%Y-%m-%d"),
        "industry": profile.get("industry"),
        "sector": profile.get("sector"),
        "marketCap": profile.get("mktCap"),  
        "roe": metrics.get("roeTTM"),
        "eps": metrics.get("netIncomePerShareTTM"),
        "bps": metrics.get("bookValuePerShareTTM"),
        "beta": profile.get("beta"),
        "dividendYield": metrics.get("dividendYieldTTM"),
        "currentRatio": metrics.get("currentRatioTTM"),
        "debtRatio": metrics.get("debtToEquityTTM"),  
    }

    # 확인용 출력
    print(f"✅ {symbol} 수집 완료 → {result}")

    # 필드 유효성 검사
    required_fields = ["eps", "roe", "currentRatio", "debtRatio"]
    missing_fields = [k for k in required_fields if result[k] is None]

    if missing_fields:
        print(f"⚠️ {symbol} 일부 필드 누락: {', '.join(missing_fields)} → None으로 처리")

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
