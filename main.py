from db import engine, SessionLocal
from models import Base
from models.stock import Stock  # 테이블 생성 대상 모델 import

def main():
    # 테이블이 없으면 생성 (이미 있으면 무시)
    Base.metadata.create_all(bind=engine)

    # 세션 생성
    db = SessionLocal()
    try:
        # 예시: Stock 테이블에 데이터 추가
        new_stock = Stock(
            symbol="AAPL",
            name="Apple Inc.",
            sector="Technology",
            industry="Consumer Electronics"
        )
        db.add(new_stock)
        db.commit()

        print("데이터 삽입 완료")

    except Exception as e:
        db.rollback()
        print(f"오류 발생: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
