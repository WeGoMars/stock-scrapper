import requests
import pymysql
import configparser
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# config 파일 로드
config = configparser.ConfigParser()
config.read('config.conf', encoding='utf-8')

# === API 설정 ===
API_KEY = os.getenv("FRED_API_KEY")
SERIES_ID = "DFEDTARU"  # 기준금리 상단
FILE_TYPE = "json"
start_date = (datetime.today() - timedelta(days=180)).date().isoformat()
end_date = datetime.today().date().isoformat()

# === API 요청 ===
url = "https://api.stlouisfed.org/fred/series/observations"
params = {
    "series_id": SERIES_ID,
    "api_key": API_KEY,
    "file_type": FILE_TYPE,
    "observation_start": start_date,
    "observation_end": end_date
}

res = requests.get(url, params=params)

if res.status_code != 200:
    print(f"❌ 요청 실패: {res.status_code}")
    exit()

data = res.json()
observations = data.get("observations", [])
if not observations:
    print("⚠️ 관측 데이터가 없습니다.")
    exit()

# === DB 설정 ===
db_config = {
    'host': config.get('database', 'host'),
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'database': config.get('database', 'name'),
    'port': config.getint('database', 'port'),
    'charset': config.get('database', 'charset'),
}

# === DB 저장 함수 ===
def store_interest_rate_data(observations):
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

    # 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS interest_rates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            series_id VARCHAR(50),
            date DATE,
            value FLOAT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT NULL,
            UNIQUE KEY unique_series_date (series_id, date)
        )
    ''')

    now = datetime.now()
    for obs in observations:
        date_str = obs.get("date")
        value_str = obs.get("value")

        try:
            value = float(value_str)
        except ValueError:
            continue  # "." (데이터 없음) 등은 건너뜀

        cursor.execute('''
            INSERT INTO interest_rates (series_id, date, value, created_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                updated_at = %s
        ''', (
            SERIES_ID, date_str, value, now, now
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 기준금리 데이터 저장 완료")

# 실행
store_interest_rate_data(observations)
