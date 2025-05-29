# collectors/fmp_collector.py

import os
import requests
from datetime import date
from dotenv import load_dotenv
from time import sleep
from itertools import cycle
from typing import List, Dict

load_dotenv()
FMP_API_KEYS = os.getenv("FMP_API_KEYS", "").split(",")
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"

# 키를 순환하는 이터레이터 생성
api_key_cycle = cycle(FMP_API_KEYS)
def get_next_api_key():
    return next(api_key_cycle)


def fetch_financial_ratios(symbol: str, limit: int = 4, max_retries: int = 3, sleep_sec: float = 0.5) -> List[Dict]:
    """
    특정 종목(symbol)의 최근 분기 재무지표 수집 (API Key 라운드로빈 + 재시도 포함)
    Returns: [{"date": "2023-12-31", "roe": 12.5, "eps": 3.45, ...}, ...]
    """
    results = []
    url = f"{FMP_BASE_URL}/ratios/{symbol}"

    for attempt in range(max_retries):
        api_key = get_next_api_key()
        params = {"apikey": api_key, "limit": limit}

        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    results.append({
                        "symbol": symbol,
                        "targetDate": item.get("date"),
                        "roe": item.get("returnOnEquity"),
                        "eps": item.get("eps"),
                        "bps": item.get("bookValuePerShare"),
                        "beta": item.get("beta"),
                        "marketCap": item.get("marketCap"),
                        "dividendYield": item.get("dividendYield"),
                        "currentRatio": item.get("currentRatio"),
                        "debtRatio": item.get("debtEquityRatio"),
                    })
                break  # 성공하면 루프 종료
            else:
                print(f"❌ {symbol} 요청 실패: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"⚠️ {symbol} 요청 예외 발생: {e}")

        sleep(sleep_sec)  # 재시도 전 대기

    if not results:
        print(f"❌ {symbol} 재무지표 수집 실패 (최대 재시도 초과)")

    return results

def fetch_stock_profiles(symbols: List[str], max_retries: int = 3, sleep_sec: float = 0.5) -> List[Dict]:
    """
    FMP API를 사용하여 종목 프로필 정보를 병렬이 아닌 순차적으로 수집하되,
    API 키를 순환하며 과부하를 분산시킴.
    Returns: [{ symbol, name, sector, industry }, ...]
    """
    results = []

    for symbol in symbols:
        url = f"{FMP_BASE_URL}/profile/{symbol}"

        for attempt in range(max_retries):
            api_key = get_next_api_key()
            params = {"apikey": api_key}

            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        profile = data[0]
                        results.append({
                            "symbol": profile.get("symbol"),
                            "name": profile.get("companyName"),
                            "sector": profile.get("sector"),
                            "industry": profile.get("industry"),
                        })
                    break  # 성공 시 루프 탈출
                else:
                    print(f"❌ {symbol} 요청 실패: {response.status_code} - {response.text} (키: {api_key})")
            except Exception as e:
                print(f"⚠️ {symbol} 요청 예외 발생: {e} (키: {api_key})")

            sleep(sleep_sec)

        else:
            print(f"❌ {symbol} 프로필 수집 실패 (최대 재시도 초과)")

    return results