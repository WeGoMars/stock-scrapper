# services/sector_service.py

from sqlalchemy.orm import Session
from collectors import fmp_financial_sector_collector
from repos.sector_repo import insert_sector_performance


def collect_sector_performance(session: Session):
    """
    Alpha Vantage를 통해 섹터별 수익률을 수집하고 저장
    """
    data = fmp_financial_sector_collector.fetch_sector_performance_from_fmp()
    if not data:
        print("❌ 수익률 수집 실패: 빈 데이터")
        return

    insert_sector_performance(session, data)
    print("✅ 섹터 수익률 저장 완료")
