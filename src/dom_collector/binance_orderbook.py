import asyncio
import json
import os
import time
from typing import Dict, Optional

import requests
import websockets
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, before_log, retry_if_exception_type

from dom_collector.logger import logger

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

WS_RECONNECT_INTERVAL = 1
WS_CONNECTION_TIMEOUT = 30
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10
WS_CLOSE_TIMEOUT = 5
WS_MONITOR_INTERVAL = 5


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
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        
        logger.info(f"Initialized BinanceOrderBook for {self.symbol} with depth limit {self.depth_limit} and max depth {self.max_depth}")
        logger.info(f"Connection settings: connection_timeout={WS_CONNECTION_TIMEOUT}s")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, max=30),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
        before=before_log(logger, "DEBUG")
    )
    async def fetch_snapshot(self) -> Dict:
        params = {"symbol": self.symbol.upper(), "limit": self.depth_limit}
        logger.debug(f"Fetching order book snapshot for {self.symbol.upper()}")
        
        response = requests.get(self.snapshot_url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

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
        logger.debug(f"Applied snapshot with lastUpdateId: {self.last_update_id}")
        logger.debug(f"Order book contains {len(self.order_book['bids'])} bids and {len(self.order_book['asks'])} asks")

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
            logger.error(f"Event gap detected: event U={event['U']} > lastUpdateId+1={self.last_update_id+1}")
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
        return True

    async def handle_websocket_message(self, websocket):
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=WS_CONNECTION_TIMEOUT)
            self.last_message_time = time.time()
            
            data = json.loads(message)
            
            if isinstance(data, dict) and "ping" in data:
                await websocket.send(json.dumps({"pong": data["ping"]}))
                self.last_heartbeat = time.time()
                return None
                
            return data
        except Exception as e:
            logger.error(f"WebSocket message error: {e}")
            self.connection_state = "reconnecting"
            return None

    async def send_heartbeat(self, websocket):
        """Sends periodic heartbeat pings to keep the connection alive."""
        while self.connection_state == "connected":
            try:
                if time.time() - self.last_heartbeat > WS_PING_INTERVAL:
                    await asyncio.wait_for(websocket.pong(b''), timeout=WS_PING_TIMEOUT)
                    self.last_heartbeat = time.time()
                    logger.debug("Sent heartbeat ping")

                await asyncio.sleep(WS_MONITOR_INTERVAL)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break

    async def connection_monitor(self):
        """Monitors connection health and triggers reconnection if needed."""
        while self.connection_state == "connected":
            try:
                await asyncio.sleep(WS_MONITOR_INTERVAL)

                time_since_last_message = time.time() - self.last_message_time
                if time_since_last_message > WS_CONNECTION_TIMEOUT:
                    logger.error(f"Connection monitor: No messages for {time_since_last_message:.1f}s")
                    self.connection_state = "reconnecting"
                    break
            except Exception as e:
                logger.error(f"Connection monitor error: {e}")
                break

    async def manage_order_book(self):
        while True:
            try:
                self.connection_state = "connecting"
                logger.info(f"Connecting to {self.ws_url}")
                
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=WS_PING_INTERVAL,
                    ping_timeout=WS_PING_TIMEOUT,
                    close_timeout=WS_CLOSE_TIMEOUT,
                    max_size=None,
                    max_queue=None,
                    ssl=True
                ) as websocket:
                    self.websocket = websocket
                    self.connection_state = "connected"
                    logger.info(f"Connected to {self.ws_url}")
                    self.last_heartbeat = time.time()
                    self.last_message_time = time.time()
                    
                    heartbeat_task = asyncio.create_task(self.send_heartbeat(websocket))
                    monitor_task = asyncio.create_task(self.connection_monitor())
                    
                    self.buffer = []
                    
                    first_event = await self.handle_websocket_message(websocket)
                    if not first_event:
                        logger.warning("Failed to get first event, reconnecting...")
                        heartbeat_task.cancel()
                        monitor_task.cancel()
                        await asyncio.sleep(WS_RECONNECT_INTERVAL)
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
                    
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                self.connection_state = "disconnected"
            
            logger.info(f"Reconnecting in {WS_RECONNECT_INTERVAL} second...")
            await asyncio.sleep(WS_RECONNECT_INTERVAL)

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