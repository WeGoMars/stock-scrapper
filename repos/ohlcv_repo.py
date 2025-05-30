from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert
from models.stock_ohlcv import StockOhlcv
from models.stock import Stock
from datetime import datetime


def insert_ohlcv_bulk(session: Session, data: list[dict]):
    """
    수집된 OHLCV 리스트를 upsert 방식으로 저장
    중복된 (stock_id, timestamp, interval) 조합이 있을 경우 update로 처리
    """
    if not data:
        return

    # symbol → stock_id 매핑
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

        # ON DUPLICATE KEY UPDATE
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
