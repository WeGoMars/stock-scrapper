from sqlalchemy.orm import Session
from collectors.yfinance_stock_profile_collector import fetch_stock_profiles_yf
from repos.stock_repo import insert_stocks
    
def collect_stock_profiles_yf(session: Session, symbols: list[str]):
    """
    yfinance를 통해 주식 기본정보(회사명, 섹터 등)를 수집하고 DB에 저장
    """
    profiles = fetch_stock_profiles_yf(symbols)
    if not profiles:
        print("❌ 종목 정보 수집 실패: 수집된 데이터가 없습니다.")
        return
    insert_stocks(session, profiles)
    print(f"✅ {len(profiles)}개 종목 프로필 저장 완료 (yfinance)")
