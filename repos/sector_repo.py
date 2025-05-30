# repos/sector_repo.py

from sqlalchemy.orm import Session
from models.sector_performance import SectorPerformance
from datetime import datetime, timezone


def insert_sector_performance(session: Session, data: list[dict]):
    today = datetime.now(timezone.utc).date()

    for row in data:
        entity = session.query(SectorPerformance).filter_by(
            date=today,
            sector=row["sector"]
        ).first()

        if entity:
            entity.return_ = row["return"]
            entity.updatedAt = datetime.now()
        else:
            entity = SectorPerformance(
                date=today,
                sector=row["sector"],
                return_=row["return"],
                createdAt=datetime.now(),
                updatedAt=datetime.now(),
            )
            session.add(entity)

    session.commit()
