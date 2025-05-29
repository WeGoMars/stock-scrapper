from db import SessionLocal
from services.market_service import collect_all_market_metrics

def main():
    session = SessionLocal()
    try:
        collect_all_market_metrics(session)
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()
