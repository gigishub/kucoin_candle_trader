import requests
import time
import pandas as pd
from datetime import datetime, timezone

class KuCoinDataFetcher:
    """
    Flexible data fetcher for KuCoin spot and futures:
    - Spot uses original API (/api/v1/market/candles) with chunked approach
    - Futures uses the KuCoin Futures /api/v1/kline/query endpoint
    """

    def __init__(self, symbol: str, timeframe: str, market_type: str, start_time: str, end_time: str):
        """
        Args:
            symbol: e.g., "BTC-USDT" or "XBTUSDM"
            timeframe: e.g., "1min", "15min", "1day" (for spot),
                       for futures pass granularity in minutes as string (e.g. "480" for 8 hours)
            market_type: "spot" or "futures"
            start_time: Start time in "%Y-%m-%d %H:%M:%S" format (UTC)
            end_time: End time in "%Y-%m-%d %H:%M:%S" format (UTC)
        """
        self.symbol = symbol.upper()
        self.timeframe = timeframe
        self.market_type = market_type.lower()
        self.start_time = start_time
        self.end_time = end_time

    def _fetch_spot_chunk(self, start_time: str, end_time: str):
        """Chunk-based spot candlesticks using KuCoin's /api/v1/market/candles."""
        time.sleep(0.2)
        url = "https://api.kucoin.com/api/v1/market/candles"
        params = {
            "type": self.timeframe,
            "symbol": self.symbol
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
        raise Exception(f"KuCoin (spot) API error: {data}")

    def _fetch_all_spot_candles(self):
        """Fetch all spot candlesticks in chunks until start_time is reached."""
        chunks = []
        current_end = self.end_time
        start_ts = int(time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")))

        while True:
            chunk = self._fetch_spot_chunk(self.start_time, current_end)
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
        # Filter by requested time range
        return [
            c for c in chunks
            if start_ts <= int(c[0]) <= int(time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")))
        ]

    def _build_spot_dataframe(self, candles):
        """Convert raw spot candle data list to a Pandas DataFrame."""
        df = pd.DataFrame(candles, columns=range(7))
        df[['timestamp', 'open', 'close', 'high', 'low']] = df[[0, 1, 2, 3, 4]]
        df.drop([0, 1, 2, 3, 4, 5, 6], axis=1, inplace=True)
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        df[['open', 'close', 'high', 'low']] = df[['open', 'close', 'high', 'low']].astype(float)
        return df

    def _fetch_futures_klines(self):
        """
        Fetch Klines from KuCoin Futures (GET /api/v1/kline/query)
        Granularity must be provided in minutes (e.g., "480" for 8 hours).
        """
        time.sleep(0.2)
        base_url = "https://api-futures.kucoin.com"
        endpoint = "/api/v1/kline/query"

        # Convert start/end to milliseconds
        from_ms = int(time.mktime(time.strptime(self.start_time, "%Y-%m-%d %H:%M:%S"))) * 1000
        to_ms = int(time.mktime(time.strptime(self.end_time, "%Y-%m-%d %H:%M:%S"))) * 1000

        params = {
            "symbol": self.symbol,
            "granularity": int(self.timeframe),  # Convert timeframe string -> int
            "from": from_ms,
            "to": to_ms
        }
        response = requests.get(f"{base_url}{endpoint}", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data or "data" not in data:
            raise Exception(f"Futures API error: {data}")
        return data["data"]

    def _build_futures_dataframe(self, candles):
        """
        Convert raw KuCoin futures klines to a Pandas DataFrame.
        Futures data is typically [timestamp, open, close, high, low, volume].
        """
        df = pd.DataFrame(candles)
        # Assuming klines format: [t, o, c, h, l, v]
        df.rename(
            columns={
                0: "timestamp",
                1: "open",
                2: "close",
                3: "high",
                4: "low",
                5: "volume",

            },
            inplace=True
        )
        df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        # Convert numeric columns
        for col in ['open', 'close', 'high', 'low', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def fetch(self):
        """
        High-level method:
        - If spot: fetch using chunk approach, build DataFrame
        - If futures: fetch using /api/v1/kline/query, build DataFrame
        """
        if self.market_type == "spot":
            candles = self._fetch_all_spot_candles()
            return self._build_spot_dataframe(candles)
        elif self.market_type == "futures":
            klines = self._fetch_futures_klines()
            return self._build_futures_dataframe(klines)
        else:
            raise ValueError("Unsupported market_type. Must be 'spot' or 'futures'.")


# Example usage:
if __name__ == "__main__":
    # Spot example
    symbol_spot = "BTC-USDT"
    timeframe_spot = "1day"
    market_type_spot = "spot"
    start_time_spot = "2021-01-01 00:00:00"
    end_time_spot = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    spot_fetcher = KuCoinDataFetcher(symbol_spot, timeframe_spot, market_type_spot, start_time_spot, end_time_spot)
    spot_df = spot_fetcher.fetch()
    print("Spot DataFrame:", spot_df.info(), spot_df.head(), sep="\n")

    # Futures example (timeframe in minutes: 1440 = 1 day)
    symbol_futures = "XBTUSDM"  
    timeframe_futures = "1440"  
    market_type_futures = "futures"
    start_time_futures = "2021-01-01 00:00:00"
    end_time_futures = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    futures_fetcher = KuCoinDataFetcher(symbol_futures, timeframe_futures, market_type_futures, start_time_futures, end_time_futures)
    futures_df = futures_fetcher.fetch()
    print("\nFutures DataFrame:", futures_df.info(), futures_df.head(), sep="\n")