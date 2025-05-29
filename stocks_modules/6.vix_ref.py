import requests
import datetime
import pymysql
import configparser
from dotenv import load_dotenv
import os

# === 설정 파일 로드 ===
load_dotenv()
config = configparser.ConfigParser()
config.read('config.conf', encoding='utf-8')

# === API 설정 ===
API_KEY = os.getenv("FRED_API_KEY")
SERIES_ID = "VIXCLS"
FILE_TYPE = "json"

# === 날짜 설정 ===
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=30)

# === API 요청 ===
url = "https://api.stlouisfed.org/fred/series/observations"
params = {
    "series_id": SERIES_ID,
    "api_key": API_KEY,
    "file_type": FILE_TYPE,
    "observation_start": start_date.isoformat(),
    "observation_end": end_date.isoformat()
}

response = requests.get(url, params=params)
data = response.json()

# === DB 연결 정보 ===
db_config = {
    'host': config.get('database', 'host'),
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'database': config.get('database', 'name'),
    'port': config.getint('database', 'port'),
    'charset': config.get('database', 'charset'),
}

# === DB 저장 함수 ===
def store_vix_data(observations):
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

    # 테이블 생성 (최초 1회만 실행됨)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fred_vix (
        id INT AUTO_INCREMENT PRIMARY KEY,
        observation_date DATE NOT NULL,
        value DECIMAL(10,4),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT NULL,
        UNIQUE KEY unique_observation_date (observation_date)
    )
    ''')

    for obs in observations:
        date = obs['date']
        value = obs['value']
        if value == ".":
            continue  # 결측값 처리

        cursor.execute("""
            INSERT INTO fred_vix (observation_date, value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value),
                updated_at = NOW()
        """, (date, float(value)))

    conn.commit()
    cursor.close()
    conn.close()

# === 실행 ===
if "observations" in data:
    print(f"✅ {len(data['observations'])}개 VIX 데이터를 가져왔습니다. DB에 저장 중...")
    store_vix_data(data["observations"])
    print("✅ 저장 완료.")
else:
    print(f"❌ 데이터 수신 실패: {data}")
