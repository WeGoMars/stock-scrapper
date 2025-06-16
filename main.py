from db import SessionLocal
from services.ohlcv_realtime_service import collect_ohlcv_intraday
from batch import run_batch_job
from datetime import datetime, date, time as dtime, timezone, timedelta
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

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

def main_loop(interval_sec: int = 3600):  # 1시간 간격 루프
    symbols = load_symbols_from_txt("./static/symbols.txt")
    last_batch_date: date | None = None
    batch_eligible_time: datetime | None = None

    while True:
        now = datetime.now(timezone.utc)
        now_date = now.date()
        intervals = ["1h"]

        status = get_us_market_status()

        # if status in ("regular", "after"):
        if status in ("regular"):
            print(f"🟢 [{now}] 시장 상태: {status} → 실시간 수집")
            session = SessionLocal()
            try:
                collect_ohlcv_intraday(session, symbols, intervals)
                # print("실시간 수집 임시 종료 상태!")
                print("✅ 실시간 수집 완료")
            except Exception as e:
                print(f"❌ 실시간 수집 오류: {e}")
                traceback.print_exc()
                session.rollback()
            finally:
                session.close()
            batch_eligible_time = None  # 수집했으니 초기화

        else:
            print(f"🔕 [{now}] 시장 상태: {status} → 배치 실행 조건 확인")

            # 오프타임 진입 시점 기록
            if batch_eligible_time is None:
                batch_eligible_time = now + timedelta(hours=1)
                print(f"⏳ 오프타임 감지 → 배치 가능 시간 예약: {batch_eligible_time}")

            # 1시간 지났고 오늘 실행 안 했으면 실행
            if now >= batch_eligible_time and last_batch_date != now_date:
                try:
                    print("🌙 배치 실행 시작")
                    run_batch_job()
                    last_batch_date = now_date
                except Exception as e:
                    print(f"❌ 배치 실행 중 오류: {e}")
                    traceback.print_exc()

        time.sleep(interval_sec)

if __name__ == "__main__":
    print("🚀 실시간 + 배치 통합 수집 루프 시작 (1시간 간격)")
    main_loop()
