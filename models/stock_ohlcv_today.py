from sqlalchemy import Column, Integer, Float, DateTime, String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.sql import func
from models import Base

class StockOhlcvToday(Base):
    __tablename__ = "stock_ohlcv_today"

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stock.id", ondelete="CASCADE"), nullable=False)
    interval = Column(String(10), nullable=False)  # '15min', '60min'
    timestamp = Column(DateTime, nullable=False)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("stock_id", "interval", name="UQ_STOCK_OHLCV_TODAY"),
        Index("IDX_STOCK_OHLCV_TODAY", "stock_id", "interval"),
    )
