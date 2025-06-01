from db import SessionLocal
from services.ohlcv_realtime_service import collect_ohlcv_intraday  # ✅ 경로 수정
from datetime import datetime, time as dtime, timezone
import time
import traceback
import signal
import sys

def get_us_market_status() -> str:
    """
    미국 주식 시장 상태 반환 (UTC 기준)
    Returns: 'pre', 'regular', 'after', or 'closed'
    """
    now = datetime.now(timezone.utc).time()
    if dtime(9, 0) <= now < dtime(14, 30):
        return "pre"
    elif dtime(14, 30) <= now < dtime(21, 0):
        return "regular"
    elif dtime(21, 0) <= now or now < dtime(1, 0):
        return "after"
    else:
        return "closed"

def load_symbols_from_txt(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [s.strip() for s in f.read().split(",") if s.strip()]

def graceful_shutdown(signum, frame):
    print(f"\n🛑 종료 시그널({signum}) 감지됨. 안전하게 종료합니다.")
    sys.exit(0)

# Ctrl+C 대응
signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

def main_loop(interval_sec: int = 900):  # 15분 주기
    symbols = load_symbols_from_txt("./static/symbols.txt")
    # symbols = symbols[:2]

    while True:
        status = get_us_market_status()
        now = datetime.now(timezone.utc)

        if status in ("pre", "regular", "after"):
            print(f"🟢 [{now}] 시장 상태: {status} → 수집 진행")
            session = SessionLocal()
            try:
                collect_ohlcv_intraday(session, symbols, ["15min", "1h"])
                print("✅ 수집 완료")
            except Exception as e:
                print(f"❌ 오류 발생: {e}")
                traceback.print_exc()
                session.rollback()
            finally:
                session.close()
        else:
            print(f"🔕 [{now}] 시장 상태: closed → 대기")

        time.sleep(interval_sec)

if __name__ == "__main__":
    print("🚀 실시간 수집 루프 시작 (15분 간격)")
    main_loop()
