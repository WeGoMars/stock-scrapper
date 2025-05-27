import requests
import datetime
from dotenv import load_dotenv
import os
import json

load_dotenv()

# === 설정 ===
API_KEY = os.getenv("FRED_API_KEY")
SERIES_ID = "VIXCLS"
FILE_TYPE = "json"

# === 날짜 설정 ===
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=30)

# === 요청 URL & 파라미터 ===
url = "https://api.stlouisfed.org/fred/series/observations"
params = {
    "series_id": SERIES_ID,
    "api_key": API_KEY,
    "file_type": FILE_TYPE,
    "observation_start": start_date.isoformat(),
    "observation_end": end_date.isoformat()
}

# === API 요청 ===
response = requests.get(url, params=params)

# === 응답 출력 ===
if response.status_code == 200:
    data = response.json()
    print("=== 전체 JSON 응답 ===")
    print(json.dumps(data, indent=2))  # 보기 좋게 출력
else:
    print(f"Request failed: {response.status_code}")