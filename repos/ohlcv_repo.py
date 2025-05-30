from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert
from models.stock_ohlcv import StockOhlcv
from models.stock import Stock
from datetime import datetime
from datetime import date
from sqlalchemy import func

def get_latest_ohlcv_timestamp(session: Session, symbol: str, interval: str) -> date | None:
    """
    해당 symbol + interval 조합의 가장 최신 timestamp 반환
    """
    stock = session.query(Stock).filter_by(symbol=symbol).first()
    if not stock:
        print(f"⚠️ {symbol} 은 stock 테이블에 존재하지 않음 → 건너뜀")
        return None

    row = session.query(func.max(StockOhlcv.timestamp)).filter(
        StockOhlcv.stock_id == stock.id,
        StockOhlcv.interval == interval
    ).scalar()

    return row  # None일 수 있음

def insert_ohlcv_bulk(session: Session, data: list[dict]):
    """
    수집된 OHLCV 리스트를 bulk upsert
    - 중복된 (stock_id, timestamp, interval) 조합이 있으면 update
    """
    if not data:
        return

    symbols = {d["symbol"] for d in data}
    stock_map = {
        stock.symbol: stock.id
        for stock in session.query(Stock).filter(Stock.symbol.in_(symbols)).all()
    }

    now = datetime.now()

    for row in data:
        symbol = row["symbol"]
        stock_id = stock_map.get(symbol)
        if not stock_id:
            print(f"⚠️ {symbol} 은 stock 테이블에 존재하지 않음 → 건너뜀")
            continue

        stmt = mysql_insert(StockOhlcv).values(
            stock_id=stock_id,
            timestamp=row["timestamp"],
            interval=row["interval"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
            createdAt=now,
            updatedAt=now,
        )

        update_stmt = stmt.on_duplicate_key_update(
            open=stmt.inserted.open,
            high=stmt.inserted.high,
            low=stmt.inserted.low,
            close=stmt.inserted.close,
            volume=stmt.inserted.volume,
            updatedAt=stmt.inserted.updatedAt,
        )
        session.execute(update_stmt)
    session.commit()