from sqlalchemy import Column, Integer, String, Date, Float, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from models import Base

class StockMarket(Base):
    __tablename__ = "stock_market"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)  # 'FEDFUNDS_AVERAGE', 'VIX', 'SNP500_DAILY_RETURN'
    timestamp = Column(Date, nullable=False)
    value = Column(Float, nullable=False)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("name", "timestamp", name="UQ_MARKET_METRIC"),
        Index("IDX_MARKET_METRIC", "name", "timestamp"),
    )
