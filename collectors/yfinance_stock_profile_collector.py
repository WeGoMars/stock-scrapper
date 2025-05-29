import yfinance as yf
from typing import List, Dict
from time import sleep

def normalize_yf_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")

def fetch_stock_profiles_yf(symbols: List[str], sleep_sec: float = 0.3) -> List[Dict]:
    """
    yfinance를 사용하여 종목의 프로필 정보를 수집합니다.

    Returns:
        [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics"
            },
            ...
        ]
    """
    results = []

    for idx, symbol in enumerate(symbols):
        print(f"🔍 [{idx+1}/{len(symbols)}] {symbol} 수집 중...")

        try:
            yf_symbol = normalize_yf_symbol(symbol)
            ticker = yf.Ticker(yf_symbol)
            # ticker = yf.Ticker(symbol)
            info = ticker.info

            profile = {
                "symbol": symbol,
                "name": info.get("shortName"),
                "sector": info.get("sector"),
                "industry": info.get("industry")
            }

            print(f"✅ {symbol} 수집 성공: {profile['name']} ({profile['sector']} / {profile['industry']})")

            results.append(profile)
        except Exception as e:
            print(f"❌ {symbol} 수집 실패: {e}")

        sleep(sleep_sec)

    return results
