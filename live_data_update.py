import pandas as pd
import json
from datetime import datetime,timezone, timedelta

from kucoin_fetch_spot import SpotDataFetcher
from ws_websocket_lib import KucoinCandlestickWebSocket


class CandleUpdate():
    def __init__(self, symbol, timeframe,bars_lookback):
        self.symbol = symbol
        self.timeframe = timeframe
        self.bars_lookback = bars_lookback
        self.start_time_str = self.calculate_start_time_with_bars().strftime("%Y-%m-%d %H:%M:%S")
        self.end_time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.fetcher = SpotDataFetcher(symbol, timeframe, self.start_time_str, self.end_time_str)
        self.ws = KucoinCandlestickWebSocket(symbol, timeframe)
        
        self.candle_list = self.fetcher.fetch_all_candles()

        self.timestamp = None
        self.open = None
        self.close = None
        self.high = None
        self.low = None
        self.volume = None
        self.turnover = None

        
    def calculate_start_time_with_bars(self):
        if self.timeframe.endswith('min'):
            delta = timedelta(minutes=int(self.timeframe[:-3]))
        elif self.timeframe.endswith('hour'):
            delta = timedelta(hours=int(self.timeframe[:-4]))
        elif self.timeframe.endswith('day'):
            delta = timedelta(days=int(self.timeframe[:-3]))
        else:
            raise ValueError("Unsupported timeframe")
        
        self.start_time = datetime.now(timezone.utc) - (self.bars_lookback * delta)
        
        return self.start_time


    def process_candle_data(self, data):
        """Process incoming candle data."""
        if data:
            # Get candle data as list
            candle = data['candles']
            self.timestamp = candle[0]
            self.open = candle[1]
            self.close = candle[2]
            self.high = candle[3]
            self.low = candle[4]
            self.volume = candle[5]
            self.turnover = candle[6]
            return candle


    def start(self):
        # Use timezone-aware objects to represent datetimes in UTC

        try:
            self.ws.start()
            while True:
                data = self.ws.get_data(timeout=1.0)
                candle = self.process_candle_data(data)
                if candle:
                    if candle[0] != self.candle_list[-1][0]:
                        print('New candle printing update')
                        # Append new candle to list
                        self.candle_list.append(candle)
                        # Ensure the list is not too long
                        if len(self.candle_list) > 500:
                            self.candle_list = self.candle_list[-500:]

                        # Create DataFrame from single candle (wrap in list for 2D structure)
                        df = pd.DataFrame(self.candle_list[-100:], columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
                        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s', utc=True)
                        df.drop(columns=['volume', 'turnover'], inplace=True)
                        print(df)

                    else:
                        # Update last candle in list
                        self.candle_list[-1] = candle
                        df_temp = pd.DataFrame([self.candle_list[-1]], columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
                        df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'].astype(int), unit='s', utc=True)
                        df_temp.drop(columns=['volume', 'turnover'], inplace=True)
                        print(df_temp)
        except Exception as e:
            print(f"Error: {e}")
            print('attempt restart')
        
        except KeyboardInterrupt:
            self.ws.stop()
    
if __name__ == "__main__":
    
    symbol = "BTC-USDT"
    timeframe = "1min"
    start_time = "2025-01-07 00:00:00"
    
    candle_update = CandleUpdate(symbol, timeframe, 100)
    candle_update.start()
