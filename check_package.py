from kucoin_candle_spot import CandleUpdate

# Example usage
candle_update = CandleUpdate(symbol="BTC-USDT", timeframe="1min", bars_lookback=100)

candle_update.ws.start()

while True:
    new_candle_df = candle_update.new_candle_update()
    if new_candle_df is not None:
        print(f'New candle:\n{new_candle_df}')
        # Trade logic for completed candles here

    inter_candle_df = candle_update.inter_candle_df()
    if inter_candle_df is not None:
        print(f'Partial candle:\n{inter_candle_df}')
        # Trade logic for partial candles here
    