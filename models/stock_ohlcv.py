from sqlalchemy import Column, Integer, Float, Date, String, ForeignKey, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from models import Base

class StockOhlcv(Base):
    __tablename__ = "stock_ohlcv"

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey("stock.id", ondelete="CASCADE"), nullable=False)
    timestamp = Column(Date, nullable=False)
    interval = Column(String(10), nullable=False)  # '1day', '1week', '1month'
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("stock_id", "timestamp", "interval", name="UQ_STOCK_OHLCV"),
        Index("IDX_STOCK_OHLCV", "stock_id", "timestamp", "interval"),
    )
