# batch_runner.py

from db import SessionLocal
from services import market_service, financial_service, stock_service, ohlcv_service, sector_service

def load_symbols_from_txt(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        line = f.readline()
        symbols = [s.strip() for s in line.split(",") if s.strip()]
        return symbols

def run_batch_job(symbol_txt_path: str = "./static/symbols.txt") -> None:
    symbols = load_symbols_from_txt(symbol_txt_path)
    symbols=symbols[:2]

    session = SessionLocal()
    try:
        print("📦 종목 기본 정보 수집 시작...")
        stock_service.collect_stock_profiles_yf(session, symbols)
        print("✅ 종목 기본 정보 수집 완료.")

        market_service.collect_all_market_metrics(session)
        print("✅ 시장 데이터 수집 완료")

        sector_service.collect_sector_performance(session)
        print("✅ 섹터 수익률 수집 완료")

        ohlcv_service.collect_ohlcv_daily(session, symbols)
        ohlcv_service.collect_ohlcv_weekly(session, symbols)
        ohlcv_service.collect_ohlcv_monthly(session, symbols)
        print("✅ OHLCV 수집 완료")

        financial_service.collect_missing_financials(session, symbols)
        print("✅ FMP 재무 데이터 수집 완료")

    except Exception as e:
        print(f"❌ 배치 오류 발생: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    run_batch_job()
