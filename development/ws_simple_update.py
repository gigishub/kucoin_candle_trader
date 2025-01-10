import websocket
import json
import logging
from datetime import datetime
import threading
import queue
import time
import requests
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionMetrics:
    """Basic connection metrics"""
    message_count: int = 0
    error_count: int = 0

class KucoinCandlestickWebSocket:
    def __init__(self, symbol: str, timeframe: str):
        """Initialize WebSocket client
        Args:
            symbol: Trading pair (e.g., "BTC-USDT")
            timeframe: Interval (e.g., "1min", "15min", "1hour")
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.api_url = "https://api.kucoin.com"
        
        # Core components
        self.ws = None
        self.running = False
        self.data_queue = queue.Queue(maxsize=100)
        self.metrics = ConnectionMetrics()
        
    def get_token(self) -> str:
        """Get WebSocket connection token"""
        response = requests.post(f"{self.api_url}/api/v1/bullet-public")
        return response.json()['data']['token']
        
    def start(self):
        """Start WebSocket connection"""
        token = self.get_token()
        self.start_connection(token)

    def on_message(self, ws, message):
        """Handle incoming messages"""
        try:
            data = json.loads(message)
            
            if data.get('type') == 'message' and 'data' in data:
                self.metrics.message_count += 1
                
                # Add receipt timestamp
                candlestick_data = data['data']
                candlestick_data['time_received'] = datetime.now().isoformat()
                
                # Store data
                try:
                    self.data_queue.put_nowait(candlestick_data)
                except queue.Full:
                    logger.warning("Queue full, dropping message")
                    
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            self.metrics.error_count += 1

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self.metrics.error_count += 1

    def on_close(self, ws, close_status_code, close_msg):
        """Handle connection close"""
        logger.info("WebSocket closed")
        
    def on_open(self, ws):
        """Handle connection open and subscribe"""
        subscribe_message = {
            "type": "subscribe",
            "topic": f"/market/candles:{self.symbol}_{self.timeframe}",
            "privateChannel": False,
            "response": True
        }
        self.ws.send(json.dumps(subscribe_message))
        logger.info(f"Subscribed to {self.symbol} candlesticks")
    
    def start_connection(self, token):
        """Start WebSocket connection"""
        self.ws = websocket.WebSocketApp(
            f"wss://ws-api-spot.kucoin.com/?token={token}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        
        self.running = True
        self.ws_thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={'ping_interval': 20}
        )
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
    def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()
            
        logger.info(f"Connection closed. Messages: {self.metrics.message_count}, Errors: {self.metrics.error_count}")

    def get_data(self, timeout=None):
        """Get next data item from queue"""
        try:
            return self.data_queue.get(timeout=timeout)
        except queue.Empty:
            return None

def main():
    # Example usage
    client = KucoinCandlestickWebSocket("BTC-USDT", "1min")
    
    try:
        client.start()
        
        # Run for 60 seconds
        end_time = time.time() + 60
        while time.time() < end_time:
            data = client.get_data(timeout=1.0)
            if data:
                print(json.dumps(data, indent=2))
                
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        client.stop()

if __name__ == "__main__":
    main()