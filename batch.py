from db import SessionLocal
from services import market_service
from services import financial_service
from services import stock_service
from services import ohlcv_service

import pandas as pd

def load_symbols_from_txt(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        line = f.readline()
        symbols = [s.strip() for s in line.split(",") if s.strip()]
        return symbols

def main():
    txt_path = "./static/symbols.txt"  # 실제 경로에 맞게 조정
    symbols = load_symbols_from_txt(txt_path)
    symbols = symbols[:2]
    
    session = SessionLocal()
    try:
        
        # 1. 종목 메타데이터 먼저 수집 (Stock 테이블)
        # print("📦 종목 기본 정보 수집 시작...")
        # collect_stock_profiles_yf(session,symbols)
        # print("✅ 종목 기본 정보 수집 완료.")
        
        # collect_all_market_metrics(session)
        # print("✅ 시장 데이터 수집 완료")

        financial_service.collect_missing_financials(session, symbols)
        print("✅ FMP 재무 데이터 수집 완료")
        
        # 일봉 수집
        # ohlcv_service.collect_ohlcv_daily(session, symbols)
        # ohlcv_service.collect_monthly_ohlcv(session,symbols)
        # ohlcv_service.collect_weekly_ohlcv(session,symbols)
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()
