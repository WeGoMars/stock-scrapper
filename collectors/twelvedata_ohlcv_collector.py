import os
import requests
from dotenv import load_dotenv
from typing import List, Dict
from itertools import cycle
from time import sleep

load_dotenv()

TWELVE_API_KEYS = os.getenv("TWELVE_API_KEYS", "").split(",")
TWELVE_API_URL = "https://api.twelvedata.com/time_series"
api_key_cycle = cycle(TWELVE_API_KEYS)


def get_next_api_key():
    return next(api_key_cycle)


def get_ohlcv_from_twelvedata(symbol: str, interval: str, limit: int = 100) -> List[Dict]:
    """
    지정된 interval로 해당 symbol의 OHLCV 데이터를 수집.
    interval: '1day', '1week', '1month', '15min', '60min'
    """
    results = []
    api_key = get_next_api_key()
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": limit,
        "apikey": api_key,
        "format": "JSON",
        "order": "desc"
    }

    try:
        response = requests.get(TWELVE_API_URL, params=params, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        if "values" not in json_data:
            print(f"❌ {symbol} ({interval}) 수집 실패: {json_data.get('message', '알 수 없는 오류')}")
            return []

        for item in json_data["values"]:
            results.append({
                "symbol": symbol,
                "interval": interval,
                "timestamp": item.get("datetime"),  # ISO 문자열
                "open": float(item.get("open", 0)),
                "high": float(item.get("high", 0)),
                "low": float(item.get("low", 0)),
                "close": float(item.get("close", 0)),
                "volume": float(item.get("volume", 0)),
            })

    except Exception as e:
        print(f"⚠️ {symbol} ({interval}) 수집 중 예외 발생: {e}")

    return results
