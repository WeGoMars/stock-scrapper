from sqlalchemy.orm import Session
from models.stock import Stock
from datetime import datetime

def insert_stocks(session: Session, stock_list: list[dict]):
    """
    종목 기본 정보를 stock 테이블에 저장 또는 갱신

    stock_list 예시:
    [
        {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "sector": "Technology",
            "industry": "Consumer Electronics"
        },
        ...
    ]
    """
    for item in stock_list:
        entity = Stock(
            symbol=item["symbol"],
            name=item["name"],
            sector=item.get("sector") or "",
            industry=item.get("industry") or "",
            createdAt=datetime.now(),
            updatedAt=datetime.now()
        )
        session.merge(entity)  # symbol에 UNIQUE 제약이 있으므로 UPSERT 가능
    session.commit()

def get_existing_symbols(session: Session, symbols: list[str]) -> set[str]:
    """
    주어진 심볼 중 이미 DB에 존재하는 심볼들을 반환
    """
    rows = session.query(Stock.symbol).filter(Stock.symbol.in_(symbols)).all()
    return {row.symbol for row in rows}