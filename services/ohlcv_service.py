from sqlalchemy.orm import Session
from collectors.twelvedata_ohlcv_collector import get_ohlcv_from_twelvedata
from repos.ohlcv_repo import insert_ohlcv_bulk
from datetime import datetime
import time


def collect_ohlcv_daily(session: Session, symbols: list[str]):
    """
    1일 단위 OHLCV 데이터를 수집하여 stock_ohlcv 테이블에 저장
    interval = '1day'
    """
    count = 1
    all_results = []
    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 일봉 데이터 수집 중...")
        count+=1
        ohlcv_data = get_ohlcv_from_twelvedata(symbol, interval="1day", limit=250)

        for row in ohlcv_data:
            row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d")  # str → date
            
        all_results.extend(ohlcv_data)
        time.sleep(1.0)

    insert_ohlcv_bulk(session, all_results)
    print(f"✅ 총 {len(all_results)}개의 일봉 OHLCV 데이터 저장 완료")

def collect_monthly_ohlcv(session: Session, symbols: list[str]):
    """
    최근 1년치 월봉 데이터를 수집하여 DB에 저장합니다.
    """
    count = 1
    all_results = []
    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 월봉 데이터 수집 중...")
        count+=1
        ohlcv_data = get_ohlcv_from_twelvedata(symbol, interval='1month', limit=12)  # 최근 12개월

        for row in ohlcv_data:
            row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d")  # str → date
            
        all_results.extend(ohlcv_data)
        time.sleep(1.0)

    insert_ohlcv_bulk(session, all_results)
    print(f"✅ 총 {len(all_results)}개의 월봉 OHLCV 데이터 저장 완료")

def collect_weekly_ohlcv(session: Session, symbols: list[str]):
    """
    최근 1년치 주봉 데이터를 수집하여 DB에 저장합니다.
    """
    count = 1
    all_results = []
    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 주봉 데이터 수집 중...")
        count+=1
        ohlcv_data = get_ohlcv_from_twelvedata(symbol, interval='1week', limit=53)  # 최근 53주

        for row in ohlcv_data:
            row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d")  # str → date
            
        all_results.extend(ohlcv_data)
        time.sleep(1.0)

    insert_ohlcv_bulk(session, all_results)
    print(f"✅ 총 {len(all_results)}개의 주봉 OHLCV 데이터 저장 완료")