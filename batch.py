from db import SessionLocal
from services.market_service import collect_all_market_metrics
from services.financial_service import collect_financials_for_symbols
from services.stock_service import collect_stock_profiles_fmp
from services.stock_service import collect_stock_profiles_yf
import pandas as pd

def load_symbols_from_txt(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        line = f.readline()
        symbols = [s.strip() for s in line.split(",") if s.strip()]
        return symbols

def main():
    txt_path = "./static/symbols.txt"  # 실제 경로에 맞게 조정
    symbols = load_symbols_from_txt(txt_path)
    
    session = SessionLocal()
    try:
        
        # 1. 종목 메타데이터 먼저 수집 (Stock 테이블)
        print("📦 종목 기본 정보 수집 시작...")
        collect_stock_profiles_yf(session,symbols)
        print("✅ 종목 기본 정보 수집 완료.")
        
        # collect_all_market_metrics(session)
        # print("✅ 시장 데이터 수집 완료")

        # collect_financials_for_symbols(session, symbols)
        # print("✅ FMP 재무 데이터 수집 완료")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()
