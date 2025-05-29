# repos/market_repo.py

from models.stock_market import StockMarket
from sqlalchemy.orm import Session
from datetime import datetime

def insert_vix_data(session: Session, vix_list: list[dict]):
    """
    VIX 데이터 리스트를 DB에 저장
    Args:
        session (Session): SQLAlchemy 세션
        vix_list (List[Dict]): [{"date": "2023-01-01", "value": 18.3}, ...]
    """
    for item in vix_list:
        entity = StockMarket(
            name="VIX",
            timestamp=item["date"],
            value=item["value"],
            createdAt=datetime.now(),  # 명시적으로 넣어주는게 안전
            updatedAt=datetime.now()
        )
        session.merge(entity)  # 자동 UPSERT (unique 제약 조건 기반)
    session.commit()

def insert_fed_rate_data(session: Session, fed_list: list[dict]):
    """
    기준금리 데이터를 stock_market 테이블에 저장 (name='FEDFUNDS_AVERAGE')
    """
    for item in fed_list:
        entity = StockMarket(
            name="FEDFUNDS_AVERAGE",
            timestamp=item["date"],
            value=item["value"],
            createdAt=datetime.now(),
            updatedAt=datetime.now()
        )
        session.merge(entity)
    session.commit()
