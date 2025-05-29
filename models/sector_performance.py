from sqlalchemy import Column, Integer, String, Float, Date, DateTime, func, UniqueConstraint
from models import Base

class SectorPerformance(Base):
    __tablename__ = "sector_performance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    sector = Column(String(255), nullable=False)
    return_ = Column("return", Float, nullable=False)  # return은 파이썬 예약어라 return_으로 우회
    createdAt = Column(DateTime, default=func.now(), nullable=False)
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("date", "sector", name="UQ_SECTOR_DATE"),
    )
