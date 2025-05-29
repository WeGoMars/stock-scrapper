from datetime import date, timedelta
from collectors.market_collector import fetch_vix_range, fetch_fed_rate_range
from repos.market_repo import insert_vix_data, insert_fed_rate_data

def collect_vix(session):
    today = date.today()
    one_year_ago = today - timedelta(days=365)
    data = fetch_vix_range(start_date=one_year_ago, end_date=today)
    insert_vix_data(session, data)
    
def collect_fed_rate(session):
    today = date.today()
    one_year_ago = today - timedelta(days=365)
    data = fetch_fed_rate_range(start_date=one_year_ago, end_date=today)
    insert_fed_rate_data(session, data)

def collect_all_market_metrics(session):
    collect_vix(session)
    collect_fed_rate(session) 
    # collect_sp500_returns(session)
