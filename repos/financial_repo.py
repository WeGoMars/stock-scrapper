# repos/financial_repo.py

from models.stock_financials import StockFinancials
from sqlalchemy.orm import Session
from datetime import datetime

def insert_financial_data(session: Session, financial_list: list[dict]):
    """
    수집된 재무 데이터를 stock_financials 테이블에 저장 (UPSERT)
    
    financial_list: [
        {
            "symbol": "AAPL",
            "targetDate": "2023-12-31",
            "roe": 12.5,
            "eps": 3.45,
            ...
        },
        ...
    ]
    """
    for item in financial_list:
        entity = StockFinancials(
            symbol=item["symbol"],
            targetDate=item["targetDate"],
            roe=item.get("roe"),
            eps=item.get("eps"),
            bps=item.get("bps"),
            beta=item.get("beta"),
            marketCap=item.get("marketCap"),
            dividendYield=item.get("dividendYield"),
            currentRatio=item.get("currentRatio"),
            debtRatio=item.get("debtRatio"),
            createdAt=datetime.now(),
            updatedAt=datetime.now()
        )
        session.merge(entity)
    session.commit()
