from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import pymysql
import os
import time

# .env 파일 로딩
load_dotenv()

# 환경변수에서 DB 연결 정보 읽기
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# SQLAlchemy 연결 문자열 구성
DATABASE_URL = (
    f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_DATABASE}?charset=utf8mb4"
)

# DB 연결 재시도 로직
for i in range(10):
    try:
        print(f"🔌 DB 연결 시도 중... ({i+1}/10)")
        test_conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            connect_timeout=3
        )
        test_conn.close()
        print("✅ DB 연결 성공")
        break
    except pymysql.err.OperationalError as e:
        print(f"⏳ 연결 실패: {e}")
        time.sleep(3)
else:
    print("❌ DB 연결 10회 실패. 종료합니다.")
    exit(1)

# SQLAlchemy 엔진 및 세션 팩토리 생성
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
