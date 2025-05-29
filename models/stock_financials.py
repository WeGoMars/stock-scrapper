from sqlalchemy import Column, Integer, Float, Date, String, ForeignKey, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from models import Base

class StockFinancials(Base):
    __tablename__ = "stock_financials"

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stock.id", ondelete="CASCADE"), nullable=False)
    targetDate = Column(Date, nullable=False)
    roe = Column(Float, nullable=True)
    eps = Column(Float, nullable=True)
    bps = Column(Float, nullable=True)
    beta = Column(Float, nullable=True)
    marketCap = Column(Float, nullable=True)
    dividendYield = Column(Float, nullable=True)
    currentRatio = Column(Float, nullable=True)
    debtRatio = Column(Float, nullable=True)
    sector = Column(String(255), nullable=True)
    industry = Column(String(255), nullable=True)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("stock_id", "targetDate", name="UQ_STOCK_FINANCIALS"),
        Index("IDX_STOCK_FINANCIALS", "stock_id", "targetDate"),
    )
