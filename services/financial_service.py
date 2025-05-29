# services/financial_service.py

from collectors.fmp_collector import fetch_financial_ratios
from repos.financial_repo import insert_financial_data
from sqlalchemy.orm import Session
from typing import List

def collect_financials_for_symbols(session: Session, symbols: List[str]):
    """
    여러 종목의 최근 분기 재무 데이터를 수집하고 DB에 저장
    """
    for symbol in symbols:
        try:
            data = fetch_financial_ratios(symbol)
            insert_financial_data(session, data)
        except Exception as e:
            print(f"❌ {symbol} 수집 실패: {e}")
