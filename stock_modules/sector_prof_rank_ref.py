import requests
import pymysql
import configparser
from datetime import datetime

# 설정 파일 로드
config = configparser.ConfigParser()
config.read('config.conf', encoding='utf-8')

# API 키
api_keys = config.get('api', 'keys').split(',')
if not api_keys:
    print("API key가 필요합니다. config.conf에 설정하세요.")
    exit(1)

api_key = api_keys[0]  # 단일 요청이므로 첫 번째 키만 사용
url = f"https://financialmodelingprep.com/api/v3/sectors-performance?apikey={api_key}"

# API 요청
res = requests.get(url)
if res.status_code != 200:
    print(f"❌ 요청 실패: {res.status_code}")
    exit()

data = res.json()
if not isinstance(data, list):
    print("⚠️ 응답 형식이 예상과 다릅니다.")
    exit()

# 퍼센트 문자열을 float로 변환
def parse_change(p):
    try:
        return float(p.replace('%', ''))
    except:
        return None

# DB 설정
db_config = {
    'host': config.get('database', 'host'),
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'database': config.get('database', 'name'),
    'port': config.getint('database', 'port'),
    'charset': config.get('database', 'charset'),
}

# DB 저장 함수
def store_sector_data(sector_list):
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
        CREATE TABLE IF NOT EXISTS sectors_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sector VARCHAR(255),
            change_percentage FLOAT,
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_sector_fetched (sector, fetched_at)
        )
    ''')

    now = datetime.now()
    for entry in sector_list:
        sector = entry.get("sector")
        change_str = entry.get("changesPercentage")
        change = parse_change(change_str)

        if sector is None or change is None:
            continue

        cursor.execute('''
            INSERT INTO sectors_performance (sector, change_percentage, fetched_at)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                change_percentage = VALUES(change_percentage),
                fetched_at = VALUES(fetched_at)
        ''', (sector, change, now))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 섹터 수익률 데이터 저장 완료")

# 실행
store_sector_data(data)
