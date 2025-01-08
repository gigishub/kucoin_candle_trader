from datetime import datetime, timezone
import time
import requests
import pandas as pd

class KuCoinDataFetcher:
    def __init__(self, symbol: str, timeframe: str, market_type: str, start_time: str, end_time: str):
        self.symbol = symbol
        self.timeframe = timeframe
        self.market_type = market_type
        self.start_time = start_time
        self.end_time = end_time

    def _fetch_candles_chunk(self, start_time: str, end_time: str):
        """Fetch a single chunk of candlestick data from KuCoin."""
        time.sleep(0.5)  # Rate-limit friendly
        base_url = "https://api.kucoin.com" if self.market_type.lower() == "spot" else "https://api-futures.kucoin.com"
        url = f"{base_url}/api/v1/market/candles"
        params = {
            "type": self.timeframe,
            "symbol": self.symbol.upper()
        }
        if start_time:
            params["startAt"] = int(time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S")))
        if end_time:
            params["endAt"] = int(time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M:%S")))

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == "200000":
            return data["data"]
        raise Exception(f"KuCoin API error: {data}")

    def _fetch_all_candles(self):
        """Fetch all candlesticks in chunks until start_time is reached."""
        chunks = []
        current_end = self.end_time
        start_ts = int(time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")))

        while True:
            chunk = self._fetch_candles_chunk(self.start_time, current_end)
            if not chunk:
                break
            earliest_ts = int(chunk[-1][0])
            chunks.extend(chunk)
            if earliest_ts <= start_ts:
                break
            current_end = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(earliest_ts - 60))

        if not chunks:
            return []
        # Sort by timestamp
        chunks.sort(key=lambda x: x[0])
        return [
            c for c in chunks 
            if start_ts <= int(c[0]) <= int(time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")))
        ]

    def _build_dataframe(self, candles):
        """Convert raw candle data list to a Pandas DataFrame."""
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        df[['open', 'close', 'high', 'low']] = df[['open', 'close', 'high', 'low']].astype(float)
        return df
    
    def no_set_index(self, candles):
        """Convert raw candle data list to a Pandas DataFrame."""
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
        # df[['timestamp', 'open', 'close', 'high', 'low']] = df[[0, 1, 2, 3, 4]]
        # df.drop([0, 1, 2, 3, 4, 5, 6], axis=1, inplace=True)
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('timestamp', inplace=True)
        #df.sort_index(inplace=True)
        df[['open', 'close', 'high', 'low']] = df[['open', 'close', 'high', 'low']].astype(float)
        return df

    def fetch(self):
        """High-level method that fetches all candles and returns a DataFrame."""
        candles = self._fetch_all_candles()
        return self._build_dataframe(candles)

# Example usage:
import json
if __name__ == "__main__":
    symbol = "BTC-USDT"
    timeframe = "1min"
    market_type = "spot"
    start_time = "2025-01-01 00:00:00"
    
    # Use timezone-aware objects to represent datetimes in UTC
    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    fetcher = KuCoinDataFetcher(symbol, timeframe, market_type, start_time, end_time)
    # df = fetcher.fetch()
    # print(df)

    list_candles = fetcher._fetch_all_candles()
    print(json.dumps(list_candles, indent=2))
