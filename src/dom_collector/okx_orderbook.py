import asyncio
import json
import os
import time
from typing import Dict, Optional

import websockets
from dotenv import load_dotenv

from dom_collector.logger import logger

load_dotenv()

OKX_API_KEY = os.getenv("OKX_API_KEY")
OKX_SECRET_KEY = os.getenv("OKX_SECRET_KEY")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE")

WS_RECONNECT_INTERVAL = 1
WS_CONNECTION_TIMEOUT = 30
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10
WS_CLOSE_TIMEOUT = 5
WS_MONITOR_INTERVAL = 5

class OKXOrderBook:
    def __init__(self, symbol: str = "BTC-USDT", depth_limit: int = 400, max_depth: int = 400):
        self.symbol = symbol.upper()
        self.depth_limit = depth_limit
        self.max_depth = max_depth
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.order_book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.last_seq_id = -1
        self.last_prev_seq_id = -1
        self.buffer = []
        self.last_heartbeat = time.time()
        self.last_message_time = time.time()
        self.connection_state = "disconnected"
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        
        logger.info(f"Initialized OKXOrderBook for {self.symbol} with depth limit {self.depth_limit} and max depth {self.max_depth}")
        logger.info(f"Connection settings: connection_timeout={WS_CONNECTION_TIMEOUT}s")

    def apply_snapshot(self, snapshot: Dict) -> None:
        self.order_book = {"bids": {}, "asks": {}}
        
        for bid in snapshot["data"][0]["bids"]:
            price, qty = float(bid[0]), float(bid[1])
            if qty > 0:
                self.order_book["bids"][price] = qty
                
        for ask in snapshot["data"][0]["asks"]:
            price, qty = float(ask[0]), float(ask[1])
            if qty > 0:
                self.order_book["asks"][price] = qty
        
        self._trim_order_book()
                
        self.last_seq_id = snapshot["data"][0]["seqId"]
        self.last_prev_seq_id = snapshot["data"][0]["prevSeqId"]
        logger.debug(f"Applied snapshot with seqId: {self.last_seq_id}")
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
        if event["data"][0]["seqId"] < self.last_seq_id:
            logger.warning(f"Skipping event with seqId={event['data'][0]['seqId']} < last_seq_id={self.last_seq_id}")
            return False
            
        if event["data"][0]["prevSeqId"] != self.last_seq_id:
            logger.error(f"Event gap detected: event prevSeqId={event['data'][0]['prevSeqId']} != last_seq_id={self.last_seq_id}")
            return False
            
        for bid in event["data"][0]["bids"]:
            price, qty = float(bid[0]), float(bid[1])
            if qty == 0:
                self.order_book["bids"].pop(price, None)
            else:
                self.order_book["bids"][price] = qty
                
        for ask in event["data"][0]["asks"]:
            price, qty = float(ask[0]), float(ask[1])
            if qty == 0:
                self.order_book["asks"].pop(price, None)
            else:
                self.order_book["asks"][price] = qty
        
        self._trim_order_book()
                
        self.last_prev_seq_id = event["data"][0]["prevSeqId"]
        self.last_seq_id = event["data"][0]["seqId"]
        return True

    async def handle_websocket_message(self, websocket):
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=WS_CONNECTION_TIMEOUT)
            self.last_message_time = time.time()
            
            data = json.loads(message)
            
            if isinstance(data, dict) and "event" in data:
                if data["event"] == "subscribe":
                    logger.info(f"Successfully subscribed to {data['arg']['channel']} for {data['arg']['instId']}")
                elif data["event"] == "error":
                    logger.error(f"Subscription error: {data['msg']}")
                return None
                
            return data
        except Exception as e:
            logger.error(f"WebSocket message error: {e}")
            self.connection_state = "reconnecting"
            return None

    async def send_heartbeat(self, websocket):
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
                    
                    subscribe_message = {
                        "op": "subscribe",
                        "args": [
                            {
                                "channel": "books",
                                "instId": self.symbol
                            }
                        ]
                    }
                    
                    await websocket.send(json.dumps(subscribe_message))
                    
                    while self.connection_state == "connected":
                        event = await self.handle_websocket_message(websocket)
                        if not event:
                            if self.connection_state == "reconnecting":
                                logger.warning("Connection monitor triggered reconnection")
                                break
                            continue
                            
                        if event["action"] == "snapshot":
                            self.apply_snapshot(event)
                        elif event["action"] == "update":
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
            "last_seq_id": self.last_seq_id,
            "last_prev_seq_id": self.last_prev_seq_id,
            "symbol": self.symbol,
            "total_bids": len(self.order_book["bids"]),
            "total_asks": len(self.order_book["asks"]),
            "connection_state": self.connection_state,
            "max_depth": self.max_depth
        }

    async def start(self):
        logger.info(f"Starting order book manager for {self.symbol}")
        await self.manage_order_book() 