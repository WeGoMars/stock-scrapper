import yfinance as yf
import datetime
import pymysql
import configparser
from datetime import datetime as dt_now

# === 설정 파일 로드 ===
config = configparser.ConfigParser()
config.read('config.conf', encoding='utf-8')

db_config = {
    'host': config.get('database', 'host'),
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'database': config.get('database', 'name'),
    'port': config.getint('database', 'port'),
    'charset': config.get('database', 'charset'),
}

# === 날짜 설정 ===
today = datetime.date.today()
past_1m = today - datetime.timedelta(days=30)
past_3m = today - datetime.timedelta(days=90)
past_6m = today - datetime.timedelta(days=180)

# === S&P500 지수 데이터 수집 ===
ticker = "^GSPC"
sp500 = yf.Ticker(ticker)
hist = sp500.history(start=past_6m, end=today)
hist_close = hist["Close"]

# === 가장 가까운 종가 찾기 ===
def get_closest_price(target_date: datetime.date) -> float:
    for offset in range(7):
        for delta in [offset, -offset]:
            check = target_date + datetime.timedelta(days=delta)
            try:
                return hist_close.loc[str(check)]
            except KeyError:
                continue
    return None

# === 수익률 계산 ===
def calc_change(current, past):
    if current is None or past is None:
        return None
    return round((current - past) / past * 100, 2)

# === 데이터 계산 ===
latest_price = get_closest_price(today)
price_1m = get_closest_price(past_1m)
price_3m = get_closest_price(past_3m)
price_6m = get_closest_price(past_6m)

returns_data = [
    ("1m", past_1m, price_1m, calc_change(latest_price, price_1m)),
    ("3m", past_3m, price_3m, calc_change(latest_price, price_3m)),
    ("6m", past_6m, price_6m, calc_change(latest_price, price_6m)),
]

# === DB 저장 ===
def store_returns_to_db(data):
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
    CREATE TABLE IF NOT EXISTS sp500_returns (
        id INT AUTO_INCREMENT PRIMARY KEY,
        period VARCHAR(10),
        base_date DATE,
        current_date DATE,
        base_price DECIMAL(10,2),
        current_price DECIMAL(10,2),
        return_percent DECIMAL(6,2),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_period_date (period, current_date)
    )
    ''')

    for period, base_date, base_price, return_pct in data:
        cursor.execute('''
            INSERT INTO sp500_returns (period, base_date, current_date, base_price, current_price, return_percent)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                base_price = VALUES(base_price),
                current_price = VALUES(current_price),
                return_percent = VALUES(return_percent)
        ''', (
            period, base_date, today, base_price, latest_price, return_pct
        ))

    conn.commit()
    cursor.close()
    conn.close()

# === 실행 ===
print("📊 S&P500 수익률 계산 결과:")
for period, base_date, base_price, return_pct in returns_data:
    print(f"{period}: 기준일({base_date}) → 수익률: {return_pct}%")

store_returns_to_db(returns_data)
