from sqlalchemy.orm import Session
from collectors.twelvedata_ohlcv_collector import get_ohlcv_from_twelvedata
from repos import ohlcv_repo
from datetime import datetime, timezone, timedelta
import time
import math


def collect_ohlcv_daily(session: Session, symbols: list[str], max_limit: int = 250, buffer_days: int = 1):
    """
    1일 단위 OHLCV 데이터를 수집하여 stock_ohlcv 테이블에 저장
    - 최근 저장된 날짜 기준으로 누락 추정하여 수집
    - buffer_days 만큼 추가하여 겹치는 데이터는 upsert 처리
    """
    count = 1
    all_results = []
    today_utc = datetime.now(timezone.utc).date()

    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 일봉 데이터 수집 중...")
        count += 1

        latest_timestamp = ohlcv_repo.get_latest_ohlcv_timestamp(session, symbol, "1day")

        if latest_timestamp:
            delta_days = (today_utc - latest_timestamp).days
            fetch_days = min(max_limit, delta_days + buffer_days)
        else:
            fetch_days = max_limit  # 최초 수집

        print(f"🔍 {symbol}: {fetch_days}일치 데이터 요청 예정")

        ohlcv_data = get_ohlcv_from_twelvedata(
            symbol,
            interval="1day",
            limit=fetch_days
        )

        if not ohlcv_data:
            continue

        for row in ohlcv_data:
            row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d").date()
            row["interval"] = "1day"
            row["symbol"] = symbol

        all_results.extend(ohlcv_data)
        time.sleep(1.0)

    ohlcv_repo.insert_ohlcv_bulk(session, all_results)
    print(f"✅ 총 {len(all_results)}개의 일봉 OHLCV 데이터 upsert 완료")
    
    
def collect_ohlcv_weekly(session: Session, symbols: list[str], max_limit: int = 100, buffer_weeks: int = 1):
    count = 1
    all_results = []
    today_utc = datetime.now(timezone.utc).date()

    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 주봉 데이터 수집 중...")
        count += 1

        latest_timestamp = ohlcv_repo.get_latest_ohlcv_timestamp(session, symbol, "1week")

        if latest_timestamp:
            delta_days = (today_utc - latest_timestamp).days
            fetch_weeks = min(max_limit, math.ceil(delta_days / 7) + buffer_weeks)
        else:
            fetch_weeks = max_limit  # 처음 수집하는 경우

        print(f"🔍 {symbol}: {fetch_weeks}주치 데이터 요청 예정")

        raw_data = get_ohlcv_from_twelvedata(symbol, interval="1week", limit=fetch_weeks)
        if not raw_data:
            continue

        for row in raw_data:
            row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d").date()
            row["interval"] = "1week"
            row["symbol"] = symbol

        all_results.extend(raw_data)
        time.sleep(1.0)

    ohlcv_repo.insert_ohlcv_bulk(session, all_results)  # 이때는 upsert 버전 사용
    print(f"✅ 총 {len(all_results)}개의 주봉 OHLCV 데이터 upsert 완료")


    
def collect_ohlcv_monthly(session: Session, symbols: list[str], max_limit: int = 60, buffer_months: int = 1):
    """
    1개월 단위 OHLCV 데이터를 수집하여 stock_ohlcv 테이블에 저장
    - 최근 저장된 날짜 기준으로 누락 추정하여 수집
    - buffer_months 만큼 추가하여 겹치는 데이터는 upsert 처리
    """
    count = 1
    all_results = []
    today_utc = datetime.now(timezone.utc).date()

    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 월봉 데이터 수집 중...")
        count += 1

        latest_timestamp = ohlcv_repo.get_latest_ohlcv_timestamp(session, symbol, "1month")

        if latest_timestamp:
            delta_days = (today_utc - latest_timestamp).days
            fetch_months = min(max_limit, math.ceil(delta_days / 30) + buffer_months)
        else:
            fetch_months = max_limit  # 처음 수집하는 경우

        print(f"🔍 {symbol}: {fetch_months}개월치 데이터 요청 예정")

        raw_data = get_ohlcv_from_twelvedata(symbol, interval="1month", limit=fetch_months)
        if not raw_data:
            continue

        for row in raw_data:
            row["timestamp"] = datetime.strptime(row["timestamp"], "%Y-%m-%d").date()
            row["interval"] = "1month"
            row["symbol"] = symbol

        all_results.extend(raw_data)
        time.sleep(1.0)

    ohlcv_repo.insert_ohlcv_bulk(session, all_results)
    print(f"✅ 총 {len(all_results)}개의 월봉 OHLCV 데이터 upsert 완료")
