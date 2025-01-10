from datetime import datetime, timezone
import time
import requests
import pandas as pd

class SpotDataFetcher:
    def __init__(self, symbol: str, timeframe: str, start_time: str, end_time: str):
        """
        Initialize the SpotDataFetcher with symbol, timeframe, start_time, and end_time.

        Args:
            symbol (str): The trading pair symbol (e.g., "BTC-USDT").
            timeframe (str): The timeframe for candlesticks (e.g., "1min").
            start_time (str): The start time for fetching data (format: "%Y-%m-%d %H:%M:%S").
            end_time (str): The end time for fetching data (format: "%Y-%m-%d %H:%M:%S").
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.start_time = start_time
        self.end_time = end_time

    def fetch_candles_chunk(self, start_time: str, end_time: str):
        """
        Fetch a single chunk of candlestick data from KuCoin.

        Args:
            start_time (str): The start time for the chunk (format: "%Y-%m-%d %H:%M:%S").
            end_time (str): The end time for the chunk (format: "%Y-%m-%d %H:%M:%S").

        Returns:
            list: A list of candlestick data.
        """
        time.sleep(0.5)  # Rate-limit friendly
        base_url = "https://api.kucoin.com"
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

    def fetch_all_candles(self):
        """
        Fetch all candlesticks in chunks until start_time is reached.

        Returns:
            list: A list of all candlestick data within the specified time range.
        """
        chunks = []
        current_end = self.end_time
        start_ts = int(time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")))
        print('Fetching candle data...')
        while True:
            chunk = self.fetch_candles_chunk(self.start_time, current_end)
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

    def build_dataframe(self, candles):
        """
        Convert raw candle data list to a Pandas DataFrame.

        Args:
            candles (list): A list of candlestick data.

        Returns:
            pd.DataFrame: A DataFrame containing the candlestick data.
        """
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        df[['open', 'close', 'high', 'low']] = df[['open', 'close', 'high', 'low']].astype(float)
        return df

    def fetch_candles_as_df(self):
        """
        High-level method that fetches all candles and returns a DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all candlestick data within the specified time range.
        """
        candles = self.fetch_all_candles()
        return self.build_dataframe(candles)

# Example usage:
if __name__ == "__main__":
    import json
    symbol = "BTC-USDT"
    timeframe = "1min"
    start_time = "2025-01-08 10:00:00"
    
    # Use timezone-aware objects to represent datetimes in UTC
    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    fetcher = SpotDataFetcher(symbol, timeframe, start_time, end_time)
    df = fetcher.fetch_candles_as_df()
    print(df)

    list_candles = fetcher.fetch_all_candles()
    print(json.dumps(list_candles, indent=2))