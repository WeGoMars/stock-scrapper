from sqlalchemy.orm import Session
from collectors.twelvedata_ohlcv_collector import get_ohlcv_from_twelvedata
from models.stock import Stock
from datetime import datetime
import time
from repos import ohlcv_today_repo  # ✅ 레포 사용

def collect_ohlcv_intraday(session: Session, symbols: list[str], intervals: list[str] = ["15min", "1h"]):
    """
    실시간 분봉 OHLCV 데이터를 수집하여 stock_ohlcv_today 테이블에 저장
    """
    count = 1
    today_utc = datetime.utcnow()
    all_results = []

    for symbol in symbols:
        print(f"📦{count}/{len(symbols)} {symbol} 분봉 수집 중...")
        count += 1

        for interval in intervals:
            rows = get_ohlcv_from_twelvedata(symbol, interval=interval, limit=1)
            if not rows:
                continue

            row = rows[0]
            try:
                timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"❌ {symbol} {interval} → timestamp 파싱 실패: {row['timestamp']}")
                continue

            all_results.append({
                "symbol": symbol,
                "interval": interval,
                "timestamp": timestamp,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            })

        time.sleep(0.5)

    # ✅ 최종 bulk insert
    ohlcv_today_repo.insert_ohlcv_today_bulk(session, all_results)
