from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from models import Base

class Stock(Base):
    __tablename__ = "stock"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    sector = Column(String(255), nullable=True)
    industry = Column(String(255), nullable=True)
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())
