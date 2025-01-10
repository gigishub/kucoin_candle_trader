import asyncio
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

        self.set_time_range()
        self.initialize_objects_needed()
        self.candle_list = self.fetcher.fetch_all_candles()
        self.df_to_trade = self.fetcher.build_dataframe(self.candle_list)
        # self.df_to_trade = self.fetcher.fetch_candles_as_df()

        self.timestamp = None
        self.open = None
        self.close = None
        self.high = None
        self.low = None
        self.volume = None
        self.turnover = None

    def set_time_range(self):
        """Calculate start time and set end time to current time."""
        self.calculate_start_time_with_bars() # Calculate start time based on timeframe and lookback period
        self.end_time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def initialize_objects_needed(self):
        """Initialize the SpotDataFetcher and KucoinCandlestickWebSocket."""
        self.fetcher = SpotDataFetcher(self.symbol, self.timeframe, self.start_time_str, self.end_time_str)
        self.ws = KucoinCandlestickWebSocket(self.symbol, self.timeframe)

    def calculate_start_time_with_bars(self):
        """Calculate the start time based on the timeframe and lookback period."""
        if self.timeframe.endswith('min'):
            delta = timedelta(minutes=int(self.timeframe[:-3]))
        elif self.timeframe.endswith('hour'):
            delta = timedelta(hours=int(self.timeframe[:-4]))
        elif self.timeframe.endswith('day'):
            delta = timedelta(days=int(self.timeframe[:-3]))
        else:
            raise ValueError("Unsupported timeframe")
        
        self.start_time = datetime.now(timezone.utc) - (self.bars_lookback * delta)
        self.start_time = self.start_time.replace(second=0, microsecond=0)
        self.start_time_str = self.start_time.strftime("%Y-%m-%d %H:%M:%S")

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
    
    def start_ws(self):
            try:
                self.ws.start()
            except Exception as e:
                print(f"Error: {e}")
    
    def stop_ws(self):
        self.ws.stop()
    
    def new_candle_update(self,include_vol_and_turnover=False):
        try:
            df = self.df_to_trade
            data = self.ws.get_data()       
            candle = self.process_candle_data(data)
            if candle:
                if candle[0] != self.candle_list[-1][0]:
                    print('New candle printing update')
                    # Append new candle to list
                    self.candle_list.append(candle)
                    # Ensure the list is not too long
                    if len(self.candle_list) > self.bars_lookback:
                        self.candle_list = self.candle_list[-self.bars_lookback:]

                    # Create DataFrame from single candle (wrap in list for 2D structure)
                    df = pd.DataFrame(self.candle_list[-self.bars_lookback:], columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s', utc=True)
                    if not include_vol_and_turnover:
                        df.drop(columns=['volume', 'turnover'], inplace=True)
                    df.set_index('timestamp', inplace=True)
                    self.df_to_trade = df
                    return self.df_to_trade
                    

                else:
                    # Update last candle in list
                    self.candle_list[-1] = candle
                    
        except Exception as e:
            print(f"Error: {e}")
            print('attempt restart')
    
    def inter_candle_df(self,include_vol_and_turnover=False):
        data = self.ws.get_data()       
        candle = self.process_candle_data(data)
        if candle:
            df_temp = pd.DataFrame([candle], columns=['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover'])
            df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'].astype(int), unit='s', utc=True)
            if not include_vol_and_turnover:
                df_temp.drop(columns=['volume', 'turnover'], inplace=True)
            df_temp.set_index('timestamp', inplace=True)
            self.inter_candle_df_att = df_temp
            return df_temp
            
        
    
if __name__ == "__main__":
    import time
    
    symbol = "BTC-USDT"
    timeframe = "1min"
    start_time = "2025-01-07 00:00:00"
    
    errorcount = 0
    try:
        candle_update = CandleUpdate(symbol, timeframe, bars_lookback=500)
        
        # start receiving contoinous candle updates through websocket access
        candle_update.start_ws()
        
        # intital candle data for calculating indicators and signals

        snapshot= candle_update.df_to_trade
        print('initial snapshot\n',snapshot)


        while True:
            # get latstest fully formed candle data
            new_candle_df = candle_update.new_candle_update()
            # if there is a new candle print the df
            if new_candle_df is not None:
                print(new_candle_df)

                # trade logic for fully formed candles here 
            
            # acces partially formed candle data
            _inter_candle_df = candle_update.inter_candle_df()
            if _inter_candle_df is not None:
                print(_inter_candle_df)
                
                # trade logic for partially formed candles here
                

    except KeyboardInterrupt:
        candle_update.stop_ws()
        print("\nShutting down gracefully...")
        
    except Exception as e:
        print(f"Error: {e}")
        print('attempt restart')
        errorcount += 1
        if errorcount > 5:
            print('Too many errors, shutting down')
            candle_update.stop_ws()
            print("\nShutting down gracefully...")



        
    
    

