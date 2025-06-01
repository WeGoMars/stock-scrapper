from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert
from models.stock_ohlcv_today import StockOhlcvToday
from models.stock import Stock
from datetime import datetime

def insert_ohlcv_today_bulk(session: Session, data: list[dict]):
    """
    실시간 분봉 OHLCV 데이터를 bulk upsert
    - 중복된 (stock_id, interval, timestamp) 조합이 있으면 update
    """
    if not data:
        return

    symbols = {d["symbol"] for d in data}
    stock_map = {
        stock.symbol: stock.id
        for stock in session.query(Stock).filter(Stock.symbol.in_(symbols)).all()
    }

    now = datetime.utcnow()
    for row in data:
        symbol = row["symbol"]
        stock_id = stock_map.get(symbol)
        if not stock_id:
            print(f"⚠️ {symbol} 은 stock 테이블에 없음 → 건너뜀")
            continue

        stmt = mysql_insert(StockOhlcvToday).values(
            stock_id=stock_id,
            interval=row["interval"],
            timestamp=row["timestamp"],
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
    print(f"✅ 총 {len(data)}개의 분봉 OHLCV 데이터 upsert 완료")
