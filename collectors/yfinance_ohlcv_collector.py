import yfinance as yf
from typing import List, Dict

def convert_yahoo_symbol(symbol: str) -> str:
    # Yahoo Finance는 . 대신 - 사용
    return symbol.replace('.', '-')

def get_ohlcv_from_yfinance(symbol: str, interval: str, limit: int = 100) -> List[Dict]:
    yf_interval_map = {
        "1day": "1d",
        "1week": "1wk",
        "1month": "1mo"
    }

    yf_period_map = {
        "1day": f"{limit}d",
        "1week": f"{limit * 7}d",
        "1month": f"{limit}mo"
    }

    if interval not in yf_interval_map:
        print(f"❌ {symbol} ({interval}) 수집 실패: yfinance는 '{interval}'를 지원하지 않음")
        return []

    yf_interval = yf_interval_map[interval]
    yf_period = yf_period_map[interval]
    
    # 심볼 변환
    yahoo_symbol = convert_yahoo_symbol(symbol)

    try:
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(period=yf_period, interval=yf_interval)

        if hist.empty:
            print(f"❌ {symbol} ({interval}) 수집 실패: 결과 없음")
            return []

        results = []
        for dt, row in hist.iterrows():
            results.append({
                "symbol": symbol,
                "interval": interval,
                "timestamp": dt.strftime("%Y-%m-%d"),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            })

        return results[::-1]

    except Exception as e:
        print(f"⚠️ {symbol} ({interval}) yfinance 수집 예외: {e}")
        return []
