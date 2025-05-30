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

def collect_fmp_stock_financials(symbol: str, target_date: datetime) -> dict | None:
    print(f"📦 수집 중: {symbol}")
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
        print(f"⚠️ {symbol}: 일부 데이터 누락 → 건너뜀")
        return None

    profile = profile[0]
    ratios = ratios[0]
    market_cap = market_cap[0]
    income = find_latest_before(income_list, target_date)
    balance = find_latest_before(balance_list, target_date)

    try:
        eps = round(float(income["netIncome"]) / float(income["weightedAverageShsOut"]), 2)
    except:
        eps = None

    try:
        equity = float(balance["totalStockholdersEquity"])
        price = float(profile["price"])
        shares = float(market_cap["marketCap"]) / price
        bps = round(equity / shares, 2)
    except:
        bps = None

    try:
        dividend_yield = round(float(profile["lastDiv"]) / float(profile["price"]) * 100, 2)
    except:
        dividend_yield = None

    try:
        current_ratio = round(float(balance["totalCurrentAssets"]) / float(balance["totalCurrentLiabilities"]), 2)
    except:
        current_ratio = None

    try:
        debt_ratio = round(float(balance["totalLiabilities"]) / float(balance["totalStockholdersEquity"]), 2)
    except:
        debt_ratio = None

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

    print(f"✅ {symbol} 수집 완료 → {result}")
    return result