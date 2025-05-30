# services/stock_service.py

from sqlalchemy.orm import Session
from collectors.yfinance_stock_profile_collector import fetch_stock_profiles_yf
from repos import stock_repo
from repos.stock_repo import insert_stocks

def collect_stock_profiles_yf(session: Session, symbols: list[str]):
    """
    yfinance를 통해 주식 기본정보(회사명, 섹터 등)를 수집하고 DB에 저장
    """
    # 1. 이미 존재하는 심볼 확인
    existing_symbols = stock_repo.get_existing_symbols(session, symbols)
    target_symbols = [s for s in symbols if s not in existing_symbols]

    for s in existing_symbols:
        print(f"⏩ {s}: 이미 존재함 → 수집 스킵")

    if not target_symbols:
        print("✅ 모든 종목이 이미 등록되어 있어 추가 수집이 필요 없습니다.")
        return

    # 2. 수집 및 저장
    profiles = fetch_stock_profiles_yf(target_symbols)
    if not profiles:
        print("❌ 종목 정보 수집 실패: 수집된 데이터가 없습니다.")
        return

    insert_stocks(session, profiles)
    print(f"✅ {len(profiles)}개 종목 프로필 저장 완료 (yfinance)")
