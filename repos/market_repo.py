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
        existing = session.query(StockMarket).filter_by(
            name="VIX",
            timestamp=item["date"]
        ).first()

        if existing:
            existing.value = item["value"]
            existing.updatedAt = datetime.now()
        else:
            entity = StockMarket(
                name="VIX",
                timestamp=item["date"],
                value=item["value"],
                createdAt=datetime.now(),
                updatedAt=datetime.now()
            )
            session.add(entity)
    session.commit()


def insert_fed_rate_data(session: Session, fed_list: list[dict]):
    """
    기준금리 데이터를 stock_market 테이블에 저장 (name='FEDFUNDS_AVERAGE')
    """
    for item in fed_list:
        existing = session.query(StockMarket).filter_by(
            name="FEDFUNDS_AVERAGE",
            timestamp=item["date"]
        ).first()

        if existing:
            existing.value = item["value"]
            existing.updatedAt = datetime.now()
        else:
            entity = StockMarket(
                name="FEDFUNDS_AVERAGE",
                timestamp=item["date"],
                value=item["value"],
                createdAt=datetime.now(),
                updatedAt=datetime.now()
            )
            session.add(entity)
    session.commit()


def insert_market_metrics(session: Session, metrics: list[dict]):
    """
    stock_market 테이블에 다수의 마켓 지표 데이터를 삽입 또는 갱신 (UPSERT)

    metrics: [
        {"name": "SNP500_1M_RETURN", "timestamp": date, "value": 2.31},
        {"name": "SNP500_2M_RETURN", "timestamp": date, "value": 4.87},
        ...
    ]
    """
    for item in metrics:
        existing = session.query(StockMarket).filter_by(
            name=item["name"],
            timestamp=item["timestamp"]
        ).first()

        if existing:
            existing.value = item["value"]
            existing.updatedAt = datetime.now()
        else:
            entity = StockMarket(
                name=item["name"],
                timestamp=item["timestamp"],
                value=item["value"],
                createdAt=datetime.now(),
                updatedAt=datetime.now()
            )
            session.add(entity)
    session.commit()
