# utils/dates.py
from datetime import date, timedelta

def get_last_business_day_of_month(target_date: date) -> date:
    """
    주어진 날짜가 포함된 달의 마지막 영업일을 반환
    """
    # 다음 달 1일 - 하루 = 말일
    next_month = target_date.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)

    # 주말이면 금요일로 당기기
    while last_day.weekday() >= 5:  # 5 = 토, 6 = 일
        last_day -= timedelta(days=1)

    return last_day
