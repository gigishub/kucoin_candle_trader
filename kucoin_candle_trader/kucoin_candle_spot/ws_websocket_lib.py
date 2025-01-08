import websocket
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import threading
import queue
import time
import requests
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

@dataclass
class ConnectionMetrics:
    """Track connection performance metrics"""
    message_count: int = 0
    error_count: int = 0
    connected_at: float = 0
    last_message_at: float = 0

class KucoinCandlestickWebSocket:
    def __init__(self, symbol: str, timeframe: str):
        """
        Initialize KuCoin WebSocket for candlestick data
        Args:
            symbol: Trading pair (e.g., "BTC-USDT")
            timeframe: Candlestick interval (e.g., "1min", "15min", "1hour")
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.api_url = "https://api.kucoin.com"
        
        # Initialize state
        self.ws = None
        self.running = False
        self.data_queue = queue.Queue(maxsize=1000)
        self.metrics = ConnectionMetrics()
        
    def _get_token(self) -> str:
        """Get WebSocket token from KuCoin API"""
        try:
            response = requests.post(f"{self.api_url}/api/v1/bullet-public")
            response.raise_for_status()
            return response.json()['data']['token']
        except Exception as e:
            logger.error(f"Failed to get token: {e}")
            raise

    def _on_message(self, _, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            
            if data.get('type') == 'message' and 'data' in data:
                self.metrics.message_count += 1
                self.metrics.last_message_at = time.time()
                
                # Add timestamp to data
                candlestick_data = data['data']
                candlestick_data['time_received'] = datetime.now().isoformat()
                
                # Put data in queue, drop if queue is full
                try:
                    self.data_queue.put_nowait(candlestick_data)
                except queue.Full:
                    logger.warning("Queue full, dropping message")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.metrics.error_count += 1

    def _on_error(self, _, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self.metrics.error_count += 1

    def _on_close(self, _, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        
    def _on_open(self, _):
        """Handle WebSocket connection open"""
        logger.info("WebSocket connected")
        self.metrics.connected_at = time.time()
        
        # Subscribe to candlestick channel
        subscribe_message = {
            "type": "subscribe",
            "topic": f"/market/candles:{self.symbol}_{self.timeframe}",
            "privateChannel": False,
            "response": True
        }
        self.ws.send(json.dumps(subscribe_message))
        logger.info(f"Subscribed to {self.symbol} candlesticks")

    def start(self):
        """Start WebSocket connection"""
        token = self._get_token()
        websocket.enableTrace(False)
        
        # Initialize WebSocket connection
        self.ws = websocket.WebSocketApp(
            f"wss://ws-api-spot.kucoin.com/?token={token}",
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open
        )
        
        self.running = True
        
        # Start WebSocket connection in a separate thread
        self.ws_thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={'ping_interval': 20, 'ping_timeout': 10}
        )
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        logger.info("WebSocket connection started")

    def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()
        
        # Log final metrics
        logger.info(
            f"Final Metrics:\n"
            f"Total messages: {self.metrics.message_count}\n"
            f"Total errors: {self.metrics.error_count}\n"
            f"Connection duration: {time.time() - self.metrics.connected_at:.1f}s"
        )

    def get_data(self, timeout: float = None) -> Optional[Dict[str, Any]]:
        """Get latest candlestick data from queue"""
        try:
            return self.data_queue.get(timeout=timeout)
        except queue.Empty:
            return None

def main():
    # Example usage
    ws_client = KucoinCandlestickWebSocket(
        symbol="BTC-USDT",
        timeframe="1min"
    )
    
    try:
        ws_client.start()
        
        # Monitor for 2 minutes
        end_time = datetime.now() + timedelta(minutes=2)
        while datetime.now() < end_time:
            data = ws_client.get_data(timeout=1.0)
            if data:
                print(json.dumps(data, indent=2))
                
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        ws_client.stop()

if __name__ == "__main__":
    main()