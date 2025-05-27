import requests
import pymysql
import time
from datetime import datetime
import configparser
import os
#import re

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('config.conf', encoding='utf-8')

# S&P 500 심볼 리스트 (예: 일부 샘플)
#symbol list 방법1
symbols_str = config.get('symbol_list', 'symbols').replace('\n','').replace('\r', '')
symbols = [s.strip() for s in symbols_str.split(',') if s.strip()]

#symbol list 방법2
# symbols_raw = config.get('symbol_list', 'symbols')
# symbols_clean = re.sub(r'\s+', '', symbols_raw)  # 모든 공백 문자 제거 (\n, \r, \t 포함)
# symbols = [s for s in symbols_clean.split(',') if s]


interval = config.get('api_options', 'interval')
outputsize = config.get('api_options', 'outputsize')
# API 키
api_keys = config.get('api', 'keys').split(',')

# API 키가 없으면 종료
if not api_keys:
    print("API keys are required. Please set them in the config file.")
    exit(1)


# API 호출 및 DB 저장 함수
def fetch_and_store_data(symbol, api_key):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": api_key
    }
    response = requests.get(url, params=params)
    data = response.json()

    if "status" in data and data["status"] != "ok":
        print(f"Error fetching data for {symbol}: {data.get('message', 'Unknown error')}")
        return

    # DB 정보
    db_config = {
        'host': config.get('database', 'host'),
        'user': config.get('database', 'user'),
        'password': config.get('database', 'password'),
        'database': config.get('database', 'name'),
        'port': config.getint('database', 'port'),
        'charset': config.get('database', 'charset'),
    }

    # 데이터베이스 저장
    conn = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        port=db_config['port'],
        charset=db_config['charset'],
        cursorclass=pymysql.cursors.DictCursor
    )

    cursor = conn.cursor()
    # 테이블 생성 (최초 한 번만 실행됨)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_prices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(10),
        datetime DATETIME,
        open DECIMAL(10,5),
        high DECIMAL(10,5),
        low DECIMAL(10,5),
        close DECIMAL(10,5),
        volume BIGINT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT NULL,
        UNIQUE KEY unique_symbol_datetime (symbol, datetime)
    )
    ''')

    for entry in data["values"]:
        dt = entry['datetime']
        open_price = float(entry['open'])
        high = float(entry['high'])
        low = float(entry['low'])
        close = float(entry['close'])
        volume = int(entry['volume'])

        cursor.execute("""
            INSERT INTO stock_prices (symbol, datetime, open, high, low, close, volume, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                updated_at = %s
        """, (
            symbol, dt, open_price, high, low, close, volume, datetime.now(), datetime.now()
        ))

    conn.commit()
    cursor.close()
    conn.close()

# 호출 제한 및 순환 설정
total_calls = config.getint('limits', 'total_calls')
calls_per_minute = config.getint('limits', 'calls_per_minute')
sleep_duration = config.getint('limits', 'sleep_duration')

key_count = len(api_keys)

for i in range(0, total_calls, calls_per_minute):  # 8개씩 묶어서 처리
    for j in range(calls_per_minute):
        call_index = i + j
        if call_index >= total_calls:
            break  # 총 호출 수 초과 방지

        symbol = symbols[call_index % len(symbols)]  # 심볼 순환
        api_key = api_keys[call_index % key_count]  # 키 순환

        print(f"Fetching data for {symbol} using API key {api_key} (Call {call_index + 1}/{total_calls})")
        fetch_and_store_data(symbol, api_key)

    if i + calls_per_minute < total_calls:  # 마지막 호출 그룹이 아니면 대기
        print("8개의 api key 요청이 완료 되었으므로, 1시간 후 사용 가능합니다.")
        time.sleep(sleep_duration)  


