# services/financial_service.py

from collectors import fmp_financial_sector_collector
from repos import financial_repo
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime

def collect_missing_financials(session: Session, symbols: List[str]):
    """
    최근 3개월 이내 재무데이터가 없는 종목에 대해서만 FMP API를 통해 수집 수행
    """
    today = datetime.today()
    month_within = 3

    for symbol in symbols:
        if financial_repo.get_recent_financials(session, symbol, month_within):
            print(f"⏩ {symbol}: 최근 데이터 존재 → 스킵")
            continue

        data = fmp_financial_sector_collector.collect_fmp_stock_financials(symbol, today)
        if data:
            # insert_financial_data()는 미리 작성된 bulk upsert용 함수로 가정
            financial_repo.insert_financials_bulk(session, [data])