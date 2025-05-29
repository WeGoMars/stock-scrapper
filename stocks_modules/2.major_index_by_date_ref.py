import requests
import pymysql
from datetime import datetime
import configparser
import time

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('config.conf', encoding='utf-8')

# 설정 정보 로드
api_keys = config.get('fmp_api', 'keys').split(',')
symbols_str = config.get('symbol_list', 'symbols').replace('\n','').replace('\r', '')
symbols = [s.strip() for s in symbols_str.split(',') if s.strip()]
target_date_str = config.get('options', 'target_date')
target_date = datetime.strptime(target_date_str, "%Y-%m-%d")

db_config = {
    'host': config.get('database', 'host'),
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'database': config.get('database', 'name'),
    'port': config.getint('database', 'port'),
    'charset': config.get('database', 'charset'),
}

base_url = "https://financialmodelingprep.com/api/v3"

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

# ===== DB 저장 함수 =====
def store_financial_data(symbol, data):
    conn = pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_financials (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(10),
        industry VARCHAR(255),
        sector VARCHAR(255),
        market_cap BIGINT,
        roe FLOAT,
        eps FLOAT,
        bps FLOAT,
        beta FLOAT,
        dividend_yield FLOAT,
        current_ratio FLOAT,
        debt_ratio FLOAT,
        target_date DATE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT NULL,
        UNIQUE KEY unique_symbol_date (symbol, target_date)
    )
    ''')

    cursor.execute('''
        INSERT INTO stock_financials (
            symbol, industry, sector, market_cap, roe, eps, bps, beta,
            dividend_yield, current_ratio, debt_ratio, target_date, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            industry = VALUES(industry),
            sector = VALUES(sector),
            market_cap = VALUES(market_cap),
            roe = VALUES(roe),
            eps = VALUES(eps),
            bps = VALUES(bps),
            beta = VALUES(beta),
            dividend_yield = VALUES(dividend_yield),
            current_ratio = VALUES(current_ratio),
            debt_ratio = VALUES(debt_ratio),
            updated_at = VALUES(updated_at)
    ''', (
        symbol,
        data.get("industry"), data.get("sector"), data.get("market_cap"),
        data.get("roe"), data.get("eps"), data.get("bps"), data.get("beta"),
        data.get("dividend_yield"), data.get("current_ratio"), data.get("debt_ratio"),
        target_date_str, datetime.now(), datetime.now()
    ))

    conn.commit()
    cursor.close()
    conn.close()

# ===== 메인 로직 =====
def fetch_and_process(symbol, api_key):
    print(f"\n📦 Fetching data for {symbol}...")
    profile_url = f"{base_url}/profile/{symbol}?apikey={api_key}"
    ratios_url = f"{base_url}/ratios-ttm/{symbol}?apikey={api_key}"
    market_cap_url = f"{base_url}/market-capitalization/{symbol}?apikey={api_key}"
    income_url = f"{base_url}/income-statement/{symbol}?limit=100&apikey={api_key}"
    balance_url = f"{base_url}/balance-sheet-statement/{symbol}?limit=100&apikey={api_key}"

    profile = fetch_json(profile_url)
    ratios = fetch_json(ratios_url)
    market_cap = fetch_json(market_cap_url)
    income_list = fetch_json(income_url)
    balance_list = fetch_json(balance_url)

    if not (profile and ratios and market_cap and income_list and balance_list):
        print(f"⚠️ {symbol}: 일부 데이터가 부족합니다.")
        return

    profile = profile[0]
    ratios = ratios[0]
    market_cap = market_cap[0]
    income = find_latest_before(income_list)
    balance = find_latest_before(balance_list)

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

    data = {
        "industry": profile.get("industry"),
        "sector": profile.get("sector"),
        "market_cap": market_cap.get("marketCap"),
        "roe": ratios.get("returnOnEquityTTM"),
        "eps": eps,
        "bps": bps,
        "beta": profile.get("beta"),
        "dividend_yield": dividend_yield,
        "current_ratio": current_ratio,
        "debt_ratio": debt_ratio
    }

    store_financial_data(symbol, data)
    print(f"✅ {symbol} 저장 완료")

# ===== 실행 제어 =====
total_calls = config.getint('limits', 'total_calls')
calls_per_minute = config.getint('limits', 'calls_per_minute')
sleep_duration = config.getint('limits', 'sleep_duration')
key_count = len(api_keys)

for i in range(0, total_calls, calls_per_minute):
    for j in range(calls_per_minute):
        call_index = i + j
        if call_index >= total_calls:
            break

        symbol = symbols[call_index % len(symbols)]
        api_key = api_keys[call_index % key_count]

        fetch_and_process(symbol, api_key)

    if i + calls_per_minute < total_calls:
        print("🕐 잠시 대기 중...")
        time.sleep(sleep_duration)
