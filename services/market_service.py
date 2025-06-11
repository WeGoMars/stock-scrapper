from datetime import datetime, timedelta, timezone, date
from collectors.fred_vix_fed_collector import fetch_vix_range, fetch_fed_rate_range
from collectors.yfinance_snp500_collector import fetch_snp500_multi_returns
from repos.market_repo import insert_vix_data, insert_fed_rate_data, insert_market_metrics
from sqlalchemy.orm import Session


def get_utc_today() -> date:
    return datetime.now(timezone.utc).date()


def collect_vix(session: Session):
    today = get_utc_today()
    one_year_ago = today - timedelta(days=365)
    data = fetch_vix_range(start_date=one_year_ago, end_date=today)
    insert_vix_data(session, data)


def collect_fed_rate(session: Session):
    today = get_utc_today()
    one_year_ago = today - timedelta(days=365)
    data = fetch_fed_rate_range(start_date=one_year_ago, end_date=today)
    insert_fed_rate_data(session, data)


def collect_snp500_multi_returns(session: Session, base_date: date = None):
    """
    기준일(base_date) 기준으로 SNP500 1M~12M 수익률을 수집하여 저장 (UTC 기준)
    """
    if base_date is None:
        base_date = get_utc_today()

    data = fetch_snp500_multi_returns(base_date)
    insert_market_metrics(session, data)


def collect_all_market_metrics(session: Session):
    collect_snp500_multi_returns(session)
    print('현재로부터 12개월의 SNP 500 수익률 수집 완료.')
    collect_vix(session)
    print('VIX 수집완료')
    collect_fed_rate(session)
    print('기준금리 수집완료')


