import asyncio
import json
import os
import time
from typing import Dict, Optional

import requests
import websockets
from dotenv import load_dotenv

from dom_collector.logger import logger

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")


class BinanceOrderBook:
    def __init__(self, symbol: str = "btcusdt", depth_limit: int = 5000, max_depth: int = 10000):
        self.symbol = symbol.lower()
        self.depth_limit = depth_limit
        self.max_depth = max_depth
        self.base_url = "https://api.binance.com"
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth"
        self.snapshot_url = f"{self.base_url}/api/v3/depth"
        self.order_book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.last_update_id = 0
        self.buffer = []
        self.last_heartbeat = time.time()
        self.last_message_time = time.time()
        self.connection_state = "disconnected"
        self.reconnect_count = 0
        
        self.max_reconnect_attempts = 10
        self.connection_timeout = 30
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        
        self.is_connected = False
        
        logger.info(f"Initialized BinanceOrderBook for {self.symbol} with depth limit {self.depth_limit} and max depth {self.max_depth}")
        logger.info(f"Connection settings: max_reconnect_attempts={self.max_reconnect_attempts}, connection_timeout={self.connection_timeout}s")

    async def fetch_snapshot(self) -> Dict:
        params = {"symbol": self.symbol.upper(), "limit": self.depth_limit}
        logger.debug(f"Fetching order book snapshot for {self.symbol.upper()}")
        
        retry_count = 0
        max_retries = 5
        retry_delay = 1
        
        while retry_count < max_retries:
            try:
                response = requests.get(self.snapshot_url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, requests.Timeout) as e:
                retry_count += 1
                logger.warning(f"Snapshot request failed (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 30)
                else:
                    logger.error(f"Failed to fetch snapshot after {max_retries} attempts")
                    raise

    def apply_snapshot(self, snapshot: Dict) -> None:
        self.order_book = {"bids": {}, "asks": {}}
        
        for bid in snapshot["bids"]:
            price, qty = float(bid[0]), float(bid[1])
            if qty > 0:
                self.order_book["bids"][price] = qty
                
        for ask in snapshot["asks"]:
            price, qty = float(ask[0]), float(ask[1])
            if qty > 0:
                self.order_book["asks"][price] = qty
        
        self._trim_order_book()
                
        self.last_update_id = snapshot["lastUpdateId"]
        logger.info(f"Applied snapshot with lastUpdateId: {self.last_update_id}")
        logger.info(f"Order book contains {len(self.order_book['bids'])} bids and {len(self.order_book['asks'])} asks")

    def _trim_order_book(self) -> None:
        if len(self.order_book["bids"]) > self.max_depth:
            sorted_bids = sorted(self.order_book["bids"].items(), reverse=True)
            self.order_book["bids"] = dict(sorted_bids[:self.max_depth])
            logger.debug(f"Trimmed bids to {self.max_depth} levels")
            
        if len(self.order_book["asks"]) > self.max_depth:
            sorted_asks = sorted(self.order_book["asks"].items())
            self.order_book["asks"] = dict(sorted_asks[:self.max_depth])
            logger.debug(f"Trimmed asks to {self.max_depth} levels")

    def process_event(self, event: Dict) -> bool:
        if event["u"] < self.last_update_id:
            logger.warning(f"Skipping event with u={event['u']} < lastUpdateId={self.last_update_id}")
            return False
            
        if event["U"] > self.last_update_id + 1:
            logger.warning(f"Event gap detected: event U={event['U']} > lastUpdateId+1={self.last_update_id+1}")
            return False
            
        for bid in event["b"]:
            price, qty = float(bid[0]), float(bid[1])
            if qty == 0:
                self.order_book["bids"].pop(price, None)
            else:
                self.order_book["bids"][price] = qty
                
        for ask in event["a"]:
            price, qty = float(ask[0]), float(ask[1])
            if qty == 0:
                self.order_book["asks"].pop(price, None)
            else:
                self.order_book["asks"][price] = qty
        
        self._trim_order_book()
                
        self.last_update_id = event["u"]
        logger.debug(f"Processed event with u={event['u']}")
        return True

    async def handle_websocket_message(self, websocket):
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=self.connection_timeout)
            self.last_message_time = time.time()
            
            if not self.is_connected:
                logger.info("Connection restored, receiving messages again")
                self.is_connected = True
                
            data = json.loads(message)
            
            if isinstance(data, dict) and "ping" in data:
                await websocket.send(json.dumps({"pong": data["ping"]}))
                self.last_heartbeat = time.time()
                return None
                
            return data
        except asyncio.TimeoutError:
            if self.is_connected:
                logger.warning(f"WebSocket message timeout after {self.connection_timeout}s - reconnecting")
                self.is_connected = False
            return None
        except websockets.exceptions.ConnectionClosed as e:
            if self.is_connected:
                logger.warning(f"WebSocket connection closed: {e}")
                self.is_connected = False
            return None
        except Exception as e:
            if self.is_connected:
                logger.error(f"Error handling WebSocket message: {e}")
                self.is_connected = False
            return None

    async def send_heartbeat(self, websocket):
        while True:
            try:
                time_since_last_message = time.time() - self.last_message_time
                if time_since_last_message > self.connection_timeout:
                    if self.is_connected:
                        logger.warning(f"No messages received for {time_since_last_message:.1f}s, connection may be stale")
                        self.is_connected = False
                    break
                
                if time.time() - self.last_heartbeat > 20:
                    try:
                        await asyncio.wait_for(websocket.pong(b''), timeout=5)
                        self.last_heartbeat = time.time()
                        logger.debug("Sent heartbeat ping")
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                        if self.is_connected:
                            logger.warning("Failed to send heartbeat, connection may be lost")
                            self.is_connected = False
                        break
                
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.debug("Heartbeat task cancelled")
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break
        
        self.connection_state = "reconnecting"

    async def connection_monitor(self):
        while True:
            try:
                await asyncio.sleep(5)
                
                time_since_last_message = time.time() - self.last_message_time
                if time_since_last_message > self.connection_timeout:
                    if self.is_connected:
                        logger.warning(f"Connection monitor: No messages for {time_since_last_message:.1f}s")
                        self.is_connected = False
                    self.connection_state = "reconnecting"
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Connection monitor error: {e}")
        
        return

    async def manage_order_book(self):
        reconnect_delay = 1
        max_reconnect_delay = 30
        
        while True:
            try:
                self.connection_state = "connecting"
                logger.info(f"Connecting to {self.ws_url} (attempt {self.reconnect_count + 1})")
                
                connection_timeout = min(30 + (self.reconnect_count * 5), 120)
                
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=None,
                    max_queue=None,
                    ssl=True
                ) as websocket:
                    self.websocket = websocket
                    self.connection_state = "connected"
                    self.is_connected = True
                    logger.info(f"Connected to {self.ws_url}")
                    self.last_heartbeat = time.time()
                    self.last_message_time = time.time()
                    self.reconnect_count = 0
                    
                    heartbeat_task = asyncio.create_task(self.send_heartbeat(websocket))
                    monitor_task = asyncio.create_task(self.connection_monitor())
                    
                    self.buffer = []
                    
                    first_event = await self.handle_websocket_message(websocket)
                    if not first_event:
                        logger.warning("Failed to get first event, reconnecting...")
                        heartbeat_task.cancel()
                        monitor_task.cancel()
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                        continue
                        
                    first_u = first_event["U"]
                    self.buffer.append(first_event)
                    
                    snapshot = await self.fetch_snapshot()
                    
                    while snapshot["lastUpdateId"] < first_u:
                        logger.warning(f"Snapshot lastUpdateId {snapshot['lastUpdateId']} < first_u {first_u}, fetching new snapshot")
                        snapshot = await self.fetch_snapshot()
                        
                    self.apply_snapshot(snapshot)
                    
                    buffering = True
                    buffer_timeout = time.time() + 5
                    
                    while buffering and time.time() < buffer_timeout:
                        try:
                            event = await asyncio.wait_for(
                                self.handle_websocket_message(websocket), 
                                timeout=0.1
                            )
                            if event:
                                self.buffer.append(event)
                        except asyncio.TimeoutError:
                            buffering = False
                    
                    logger.info(f"Processing {len(self.buffer)} buffered events")
                    valid_events = 0
                    for event in self.buffer:
                        if event["u"] <= snapshot["lastUpdateId"]:
                            continue
                        if self.process_event(event):
                            valid_events += 1
                        else:
                            logger.error("Invalid event in buffer, restarting")
                            break
                    
                    logger.info(f"Processed {valid_events} valid events from buffer")
                    
                    self.buffer = []
                    
                    logger.info("Starting to process live events")
                    reconnect_delay = 1
                    
                    while self.connection_state == "connected":
                        event = await self.handle_websocket_message(websocket)
                        if not event:
                            if self.connection_state == "reconnecting":
                                logger.warning("Connection monitor triggered reconnection")
                                break
                            continue
                            
                        if not self.process_event(event):
                            logger.error("Invalid live event, restarting")
                            self.connection_state = "reconnecting"
                            break
                    
                    heartbeat_task.cancel()
                    monitor_task.cancel()
                    try:
                        await heartbeat_task
                        await monitor_task
                    except asyncio.CancelledError:
                        pass
                    
            except (websockets.exceptions.ConnectionClosed, 
                    websockets.exceptions.InvalidStatusCode,
                    websockets.exceptions.InvalidMessage,
                    ConnectionRefusedError,
                    ConnectionResetError,
                    ConnectionError,
                    OSError) as e:
                if self.is_connected:
                    logger.warning(f"WebSocket connection error: {e}")
                    self.is_connected = False
                self.connection_state = "disconnected"
            except asyncio.CancelledError:
                logger.info("Order book manager task cancelled")
                break
            except Exception as e:
                if self.is_connected:
                    logger.error(f"WebSocket error: {e}")
                    self.is_connected = False
                self.connection_state = "disconnected"
            
            self.reconnect_count += 1
            if self.reconnect_count > self.max_reconnect_attempts:
                logger.warning(f"Reached maximum reconnection attempts ({self.max_reconnect_attempts}), resetting counter and increasing delay")
                reconnect_delay = min(reconnect_delay * 2, 120)
                self.reconnect_count = 0
            
            logger.info(f"Reconnecting in {reconnect_delay} seconds... (attempt {self.reconnect_count}/{self.max_reconnect_attempts})")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    def get_full_order_book(self) -> Dict:
        return {
            "bids": dict(sorted(self.order_book["bids"].items(), reverse=True)),
            "asks": dict(sorted(self.order_book["asks"].items())),
            "lastUpdateId": self.last_update_id,
            "symbol": self.symbol.upper(),
            "total_bids": len(self.order_book["bids"]),
            "total_asks": len(self.order_book["asks"]),
            "connection_state": self.connection_state,
            "max_depth": self.max_depth
        }

    async def start(self):
        logger.info(f"Starting order book manager for {self.symbol.upper()}")
        await self.manage_order_book() 