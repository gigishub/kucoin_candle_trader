import pandas as pd
import json
from datetime import datetime, timezone

from .fetch_spot import KuCoinDataFetcher
from .ws_websocket_lib import KucoinCandlestickWebSocket

class LiveDataUpdater:
    def __init__(self, symbol, timeframe, market_type, start_time, end_time, max_candles=500):
        self.symbol = symbol
        self.timeframe = timeframe
        self.market_type = market_type
        self.start_time = start_time
        self.end_time = end_time
        self.max_candles = max_candles
        self.candle_list = []
        self.ws = KucoinCandlestickWebSocket(symbol, timeframe)
        self.fetcher = KuCoinDataFetcher(symbol, timeframe, market_type, start_time, end_time)
        self.candle_list = self.fetcher._fetch_all_candles()
        self.open = None
        self.close = None
        self.high = None
        self.low = None

    def process_data(self, data):
        if data:
            # Get candle data as list
            candle = data['candles']
            self.open = candle[1]
            self.close = candle[2]
            self.high = candle[3]
            self.low = candle[4]

            if not self.candle_list or candle[0] != self.candle_list[-1][0]:
                print('New candle printing update')
                self.add_new_candle(candle)
            else:
                self.update_last_candle(candle)

    def add_new_candle(self, candle):
        # Append new candle to list
        self.candle_list.append(candle)
        # Ensure the list is not too long
        if len(self.candle_list) > self.max_candles:
            self.candle_list = self.candle_list[-self.max_candles:]

        # Create DataFrame from the last 100 candles
        df = pd.DataFrame(self.candle_list[-100:], columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s', utc=True)
        df.drop(columns=['volume', 'turnover'], inplace=True)
        print(df)

    def update_last_candle(self, candle):
        # Update last candle in list
        self.candle_list[-1] = candle
        df_temp = pd.DataFrame([self.candle_list[-1]], columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
        df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'].astype(int), unit='s', utc=True)
        df_temp.drop(columns=['volume', 'turnover'], inplace=True)
        print(df_temp)

    def start(self):
        try:
            self.ws.start()
            while True:
                data = self.ws.get_data(timeout=1.0)
                self.process_data(data)
        except KeyboardInterrupt:
            self.ws.stop()

# Example usage
if __name__ == "__main__":
    symbol = "BTC-USDT"
    timeframe = "1min"
    market_type = "spot"
    start_time = "2025-01-07 00:00:00"
    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    updater = LiveDataUpdater(symbol, timeframe, market_type, start_time, end_time)
    updater.start()