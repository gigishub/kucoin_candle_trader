import asyncio
import aiohttp
import orjson
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import time

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s - %(funcName)s', datefmt='%H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.propagate = False

class KucoinCandlestickWebSocket:
    """
    KuCoin WebSocket client specifically for candlestick data
    """
    def __init__(self, symbol: str, timeframe: str):
        """
        Initialize websocket connection for candlestick data
        Args:
            symbol: Trading pair in format like "BTC-USDT"
            timeframe: Candlestick timeframe (e.g., "1min", "5min", "15min", "1hour", "1day")
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.api_url = "https://api.kucoin.com"
        
        # State management
        self.queue = asyncio.Queue(maxsize=100000)
        self._last_heartbeat = time.monotonic()
        self._connection_ready = asyncio.Event()
        self._warm_up_complete = asyncio.Event()
        self._start_time = None
        self.is_running = False
        self.ws_connection = None
        self.ws_session = None

    def _prepare_subscription_data(self) -> dict:
        """Prepare subscription data for candlestick channel"""
        return {
            "type": "subscribe",
            "topic": f"/market/candles:{self.symbol}_{self.timeframe}",
            "privateChannel": False,
            "response": True
        }

    async def get_token(self) -> Optional[str]:
        """Get WebSocket token"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.api_url}/api/v1/bullet-public",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        data = await response.json(loads=orjson.loads)
                        return data['data']['token']
                    logger.error(f"Token retrieval failed with status: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Token retrieval error: {e}")
            return None

    async def _process_message(self, msg_data: dict) -> None:
        """Process incoming WebSocket messages"""
        try:
            precise_time = datetime.now()
            
            if msg_data.get('type') == 'message' and 'data' in msg_data:
                processed_data = msg_data['data']
                processed_data['time_received'] = precise_time.strftime('%H:%M:%S.%f')[:-3]
                
                try:
                    self.queue.put_nowait(processed_data)
                except asyncio.QueueFull:
                    logger.warning("Queue is full, dropping message")
            
            elif msg_data.get('type') == 'pong':
                self._last_heartbeat = time.monotonic()
                
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    async def start(self):
        """Start WebSocket connection"""
        try:
            self._start_time = time.monotonic()
            await self._start_websocket()
        except Exception as e:
            logger.error(f"Start error: {e}")
            raise

    async def _start_websocket(self):
        """Initialize and maintain WebSocket connection"""
        try:
            token = await self.get_token()
            if not token:
                raise Exception("Failed to obtain WebSocket token")

            self.ws_session = aiohttp.ClientSession()
            self.ws_connection = await self.ws_session.ws_connect(
                f"wss://ws-api-spot.kucoin.com/?token={token}",
                heartbeat=20,
                receive_timeout=30
            )

            # Start background tasks
            self.is_running = True
            asyncio.create_task(self._keep_alive())
            
            # Send subscription
            subscription_data = self._prepare_subscription_data()
            await self.ws_connection.send_str(orjson.dumps(subscription_data).decode('utf-8'))
            self._connection_ready.set()
            
            logger.info(f"WebSocket connected and subscribed to candlesticks for {self.symbol}")
            
            await self._message_loop()

        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            await self.cleanup()
            raise

    async def _message_loop(self):
        """Main message processing loop"""
        while self.is_running:
            try:
                msg = await self.ws_connection.receive(timeout=0.1)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_message(orjson.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logger.warning(f"WebSocket state changed: {msg.type}")
                    break
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Message loop error: {e}")
                break

    async def _keep_alive(self):
        """Maintain connection heartbeat"""
        while self.is_running:
            try:
                if time.monotonic() - self._last_heartbeat > 15:
                    await self.ws_connection.ping()
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Keep-alive error: {e}")
                break

    async def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        
        if self.ws_session and not self.ws_session.closed:
            await self.ws_session.close()

    async def get_data(self) -> Dict[str, Any]:
        """Get latest candlestick data from the queue"""
        try:
            return await self.queue.get()
        except asyncio.QueueEmpty:
            return None

async def main():
    # Example usage with configurable timeframe
    import json
    ws = KucoinCandlestickWebSocket(
        symbol="BTC-USDT",
        timeframe="1min"  # Can be changed to any valid timeframe
    )
    
    try:
        asyncio.create_task(ws.start()) 
        
        # Monitor for 2 minutes
        end_time = datetime.now() + timedelta(minutes=2)
        while datetime.now() < end_time:
            data = await ws.get_data()
            if data:
                print(json.dumps(data, indent=4))
            await asyncio.sleep(1)
            
    finally:
        await ws.cleanup()

if __name__ == "__main__":
    asyncio.run(main())