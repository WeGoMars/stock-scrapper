# repos/financial_repo.py

from sqlalchemy.orm import Session
from models.stock import Stock
from models.stock_financials import StockFinancials
from datetime import date


def get_recent_financials(session: Session, symbol: str, within_months: int = 3) -> list[StockFinancials]:
    """
    특정 종목(symbol)에 대해 최근 N개월 이내 수집된 재무정보 조회
    """
    stock = session.query(Stock).filter(Stock.symbol == symbol).first()
    if not stock:
        return []

    threshold_date = date.today().replace(day=1)
    month = threshold_date.month - within_months
    year = threshold_date.year
    while month <= 0:
        month += 12
        year -= 1
    threshold_date = threshold_date.replace(year=year, month=month)

    return session.query(StockFinancials).filter(
        StockFinancials.stock_id == stock.id,
        StockFinancials.targetDate >= threshold_date
    ).all()


def insert_financials_bulk(session: Session, data: list[dict]) -> None:
    """
    여러 개의 재무정보를 bulk insert
    data: {symbol, targetDate, roe, eps, bps, ...} 형태
    """
    if not data:
        return

    # symbol → stock_id 매핑
    symbols = {d["symbol"] for d in data}
    stock_map = {
        stock.symbol: stock.id
        for stock in session.query(Stock).filter(Stock.symbol.in_(symbols)).all()
    }

    now = date.today()
    entities = []
    for row in data:
        stock_id = stock_map.get(row["symbol"])
        if not stock_id:
            continue

        entities.append(StockFinancials(
            stock_id=stock_id,
            targetDate=row["targetDate"],
            roe=row.get("roe"),
            eps=row.get("eps"),
            bps=row.get("bps"),
            beta=row.get("beta"),
            marketCap=row.get("marketCap"),
            dividendYield=row.get("dividendYield"),
            currentRatio=row.get("currentRatio"),
            debtRatio=row.get("debtRatio"),
            sector=row.get("sector"),
            industry=row.get("industry"),
        ))

    session.bulk_save_objects(entities)
    session.commit()
