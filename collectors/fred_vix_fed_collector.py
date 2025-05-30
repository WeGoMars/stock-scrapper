# collectors/market_collector.py

import os
import requests
from dotenv import load_dotenv
from datetime import date

load_dotenv()  # .env에서 FRED_API_KEY 읽기

FRED_API_KEY = os.getenv("FRED_API_KEY")

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

def fetch_vix_range(start_date: date, end_date: date) -> list[dict]:
    """
    FRED API로부터 VIX 데이터를 조회하여 날짜-값 dict 리스트로 반환

    Returns:
        List[Dict]: [{"date": "2023-01-01", "value": 18.3}, ...]
    """
    params = {
        "series_id": "VIXCLS",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
    }

    response = requests.get(FRED_URL, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"FRED 요청 실패: {response.status_code} {response.text}")

    data = response.json()
    observations = data.get("observations", [])

    result = []
    for obs in observations:
        date_str = obs.get("date")
        value_str = obs.get("value")

        if value_str == ".":
            continue  # 결측값 무시

        try:
            value = float(value_str)
            result.append({"date": date_str, "value": value})
        except ValueError:
            continue

    return result


def fetch_fed_rate_range(start_date: date, end_date: date) -> list[dict]:
    """
    FRED API에서 기준금리 상단 (DFEDTARU) 데이터를 수집
    Returns: [{"date": "2023-01-01", "value": 5.25}, ...]
    """
    params = {
        "series_id": "DFEDTARU",  # 기준금리 상단
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
    }

    response = requests.get(FRED_URL, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"FRED 요청 실패: {response.status_code} {response.text}")

    data = response.json()
    observations = data.get("observations", [])

    result = []
    for obs in observations:
        date_str = obs.get("date")
        value_str = obs.get("value")

        if value_str == ".":
            continue

        try:
            value = float(value_str)
            result.append({"date": date_str, "value": value})
        except ValueError:
            continue

    return result