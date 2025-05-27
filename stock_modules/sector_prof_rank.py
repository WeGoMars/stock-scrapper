import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FMP_API_KEY")
url = f"https://financialmodelingprep.com/api/v3/sectors-performance?apikey={API_KEY}"

res = requests.get(url)
if res.status_code != 200:
    print(f"❌ 요청 실패: {res.status_code}")
    exit()

sector_list = res.json()  # 최상위가 리스트이므로 .get() 안 씀

# 퍼센트 문자열 제거하고 정렬
def parse_change(p):
    try:
        return float(p.replace('%', ''))
    except:
        return -9999

# 상위 3개 섹터 추출
sorted_sectors = sorted(sector_list, key=lambda x: parse_change(x["changesPercentage"]), reverse=True)
top_3 = sorted_sectors[:3]

# 결과 출력
print("✅ 상위 3개 섹터 수익률 랭킹:")
print(json.dumps(top_3, indent=2))