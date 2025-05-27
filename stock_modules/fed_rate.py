import requests
import datetime
import json
from dotenv import load_dotenv
import os

load_dotenv()

# === 설정 ===
API_KEY = os.getenv("FRED_API_KEY")
SERIES_ID = "DFEDTARU"  # 기준금리 상단
FILE_TYPE = "json"

start_date = (datetime.date.today() - datetime.timedelta(days=180)).isoformat()
end_date = datetime.date.today().isoformat()

# === API 요청 ===
url = "https://api.stlouisfed.org/fred/series/observations"
params = {
    "series_id": SERIES_ID,
    "api_key": API_KEY,
    "file_type": FILE_TYPE,
    "observation_start": start_date,
    "observation_end": end_date
}

res = requests.get(url, params=params)

if res.status_code != 200:
    print(f"❌ 요청 실패: {res.status_code}")
    exit()

# === 전체 JSON 응답 그대로 보기 좋게 출력 ===
data = res.json()
print("=== 전체 FRED 응답 ===")
print(json.dumps(data, indent=2))