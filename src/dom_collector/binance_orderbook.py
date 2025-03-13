"""Binance order book implementation."""

import asyncio
import json
import os
import time
from typing import Dict

import requests
import websockets
from dotenv import load_dotenv

from dom_collector.logger import logger

load_dotenv()

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")


class BinanceOrderBook:
    def __init__(self, symbol: str = "btcusdt", depth_limit: int = 5000):
        self.symbol = symbol.lower()
        self.depth_limit = depth_limit
        self.base_url = "https://api.binance.com"
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth"
        self.snapshot_url = f"{self.base_url}/api/v3/depth"
        self.order_book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.last_update_id = 0
        self.buffer = []
        self.is_processing = False
        self.last_heartbeat = time.time()
        logger.info(f"Initialized BinanceOrderBook for {self.symbol} with depth limit {self.depth_limit}")

    async def fetch_snapshot(self) -> Dict:
        params = {"symbol": self.symbol.upper(), "limit": self.depth_limit}
        logger.debug(f"Fetching order book snapshot for {self.symbol.upper()}")
        response = requests.get(self.snapshot_url, params=params)
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
                
        self.last_update_id = snapshot["lastUpdateId"]
        logger.info(f"Applied snapshot with lastUpdateId: {self.last_update_id}")

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
                
        self.last_update_id = event["u"]
        logger.debug(f"Processed event with u={event['u']}")
        return True

    async def handle_websocket_message(self, websocket):
        try:
            message = await websocket.recv()
            data = json.loads(message)
            
            if isinstance(data, dict) and "ping" in data:
                await websocket.send(json.dumps({"pong": data["ping"]}))
                self.last_heartbeat = time.time()
                return None
                
            return data
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
            return None

    async def send_heartbeat(self, websocket):
        while True:
            try:
                if time.time() - self.last_heartbeat > 20:
                    await websocket.pong(b'')
                    self.last_heartbeat = time.time()
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break

    async def manage_order_book(self):
        reconnect_delay = 1
        max_reconnect_delay = 30
        
        while True:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    logger.info(f"Connected to {self.ws_url}")
                    self.last_heartbeat = time.time()
                    
                    heartbeat_task = asyncio.create_task(self.send_heartbeat(websocket))
                    
                    self.buffer = []
                    
                    first_event = await self.handle_websocket_message(websocket)
                    if not first_event:
                        logger.warning("Failed to get first event, reconnecting...")
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
                    
                    while True:
                        event = await self.handle_websocket_message(websocket)
                        if not event:
                            continue
                            
                        if not self.process_event(event):
                            logger.error("Invalid live event, restarting")
                            break
                            
                    heartbeat_task.cancel()
                    
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                
                logger.info(f"Reconnecting in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    def get_order_book(self, limit: int = 10) -> Dict:
        return {
            "bids": dict(sorted(self.order_book["bids"].items(), reverse=True)[:limit]),
            "asks": dict(sorted(self.order_book["asks"].items())[:limit]),
            "lastUpdateId": self.last_update_id,
            "symbol": self.symbol.upper(),
            "total_bids": len(self.order_book["bids"]),
            "total_asks": len(self.order_book["asks"])
        }

    def get_full_order_book(self) -> Dict:
        return {
            "bids": dict(sorted(self.order_book["bids"].items(), reverse=True)),
            "asks": dict(sorted(self.order_book["asks"].items())),
            "lastUpdateId": self.last_update_id,
            "symbol": self.symbol.upper(),
            "total_bids": len(self.order_book["bids"]),
            "total_asks": len(self.order_book["asks"])
        }

    async def start(self):
        logger.info(f"Starting order book manager for {self.symbol.upper()}")
        await self.manage_order_book()


async def main():
    symbol = "btcusdt"
    logger.info(f"Initializing order book for {symbol}")
    order_book = BinanceOrderBook(symbol)
    
    order_book_task = asyncio.create_task(order_book.start())
    
    try:
        while True:
            await asyncio.sleep(1)
            current_book = order_book.get_order_book()
            logger.info(f"Order Book for {current_book['symbol']} (Update ID: {current_book['lastUpdateId']})")
            logger.info(f"Total Bids: {current_book['total_bids']}, Total Asks: {current_book['total_asks']}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        order_book_task.cancel()


if __name__ == "__main__":
    asyncio.run(main()) 