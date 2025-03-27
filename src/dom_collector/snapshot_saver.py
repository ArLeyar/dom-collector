import os
import time
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

import polars as pl
import boto3
from dotenv import load_dotenv

from dom_collector.logger import logger

load_dotenv()


class OrderBookSnapshotSaver:
    
    def __init__(
        self,
        output_dir: str = "snapshots",
        save_interval_seconds: int = 3600, 
        save_to_spaces: bool = False,
        retention_hours: int = 24,
        exchange: str = "unknown",
    ):
        self.output_dir = output_dir
        self.save_interval_seconds = save_interval_seconds
        self.current_file_index = 0
        self.save_to_spaces = save_to_spaces
        self.spaces_bucket = None
        self.snapshot_queue = asyncio.Queue()
        self.consumer_task = None
        self.running = False
        self.last_save_time = time.time()
        self.file_start_time = time.time()
        self.retention_hours = retention_hours
        self.exchange = exchange.lower()
        
        if self.save_to_spaces:
            self._init_spaces_client()
        
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Initialized OrderBookSnapshotSaver with output directory: {output_dir}")
        logger.info(f"Save interval: {save_interval_seconds} seconds")
        logger.info(f"File retention period: {retention_hours} hours")
        logger.info(f"Exchange: {self.exchange}")
        if self.save_to_spaces and hasattr(self, 'spaces_client'):
            logger.info(f"Digital Ocean Spaces enabled with bucket: {self.spaces_bucket}")
    
    def _init_spaces_client(self):
        try:
            region = os.getenv("DO_SPACES_REGION")
            endpoint_url = os.getenv("DO_SPACES_ENDPOINT_URL")
            spaces_key = os.getenv("DO_SPACES_KEY")
            spaces_secret = os.getenv("DO_SPACES_SECRET")
            self.spaces_bucket = os.getenv("DO_SPACES_BUCKET")
            
            if not all([region, endpoint_url, spaces_key, spaces_secret, self.spaces_bucket]):
                logger.error("Missing Digital Ocean Spaces configuration")
                self.save_to_spaces = False
                return
            
            session = boto3.session.Session()
            self.spaces_client = session.client(
                's3',
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=spaces_key,
                aws_secret_access_key=spaces_secret
            )
            logger.info(f"Successfully initialized Digital Ocean Spaces client for region {region}")
        except Exception as e:
            logger.error(f"Failed to initialize Digital Ocean Spaces client: {e}")
            self.save_to_spaces = False
    
    @property
    def pending_snapshots(self):
        return self.snapshot_queue.qsize()
    
    async def start(self):
        if self.running:
            return
        
        self.running = True
        self.consumer_task = asyncio.create_task(self._consumer())
        logger.info("Started OrderBookSnapshotSaver consumer task")
    
    async def stop(self):
        if not self.running:
            return
        
        self.running = False
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        
        if self.snapshot_queue.qsize() > 0:
            await self._save_snapshots_from_queue()
        
        logger.info("Stopped OrderBookSnapshotSaver consumer task")
    
    async def add_snapshot(self, order_book: Dict[str, Any], timestamp: Optional[float] = None):
        if timestamp is None:
            timestamp = time.time()
        
        snapshot = order_book.copy()
        snapshot['timestamp'] = timestamp
        
        await self.snapshot_queue.put(snapshot)
        
        symbol = order_book.get("symbol", "UNKNOWN")
        update_id = order_book.get("lastUpdateId", order_book.get("last_seq_id", 0))
        logger.debug(f"Added snapshot for {symbol} with update_id {update_id}")
    
    async def _consumer(self):
        try:
            while self.running:
                current_time = time.time()
                queue_size = self.snapshot_queue.qsize()
                
                time_since_file_start = current_time - self.file_start_time
                if time_since_file_start >= self.save_interval_seconds and queue_size > 0:
                    await self._save_snapshots_from_queue()
                    self.last_save_time = current_time
                    self.file_start_time = current_time 
                    self.current_file_index += 1
                
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.debug("Consumer task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in consumer task: {e}")
    
    def _cleanup_old_files(self):
        try:
            now = time.time()
            retention_seconds = self.retention_hours * 3600
            
            parquet_files = [f for f in os.listdir(self.output_dir) 
                            if f.endswith('.parquet')]
            
            deleted_count = 0
            for filename in parquet_files:
                filepath = os.path.join(self.output_dir, filename)
                file_mtime = os.path.getmtime(filepath)
                
                if now - file_mtime > retention_seconds:
                    os.remove(filepath)
                    deleted_count += 1
                    logger.info(f"Deleted old file: {filename}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old files")
        except Exception as e:
            logger.error(f"Error cleaning up old files: {e}")
    
    async def _save_snapshots_from_queue(self):
        if self.snapshot_queue.empty():
            logger.debug("No snapshots to save")
            return None
        
        try:
            first_snapshot = None
            snapshot_count = 0
            
            schema = {
                "timestamp": pl.Float64,
                "symbol": pl.Utf8,
                "update_id": pl.Int64,
                "side": pl.Utf8,
                "level": pl.Int32,
                "price": pl.Float64,
                "quantity": pl.Float64
            }
            
            df = pl.DataFrame(schema=schema)
            
            while not self.snapshot_queue.empty():
                try:
                    snapshot = self.snapshot_queue.get_nowait()
                    
                    if first_snapshot is None:
                        first_snapshot = snapshot
                    
                    snapshot_count += 1
                    
                    timestamp = snapshot.get("timestamp", time.time())
                    symbol = snapshot.get("symbol", "UNKNOWN")
                    update_id = snapshot.get("lastUpdateId", snapshot.get("last_seq_id", 0))
                    bids = snapshot.get("bids", {})
                    asks = snapshot.get("asks", {})
                    
                    records = []
                    
                    for i, (price, qty) in enumerate(bids.items()):
                        records.append({
                            "timestamp": timestamp,
                            "symbol": symbol,
                            "update_id": update_id,
                            "side": "bid",
                            "level": i + 1,
                            "price": float(price),
                            "quantity": float(qty)
                        })
                    
                    for i, (price, qty) in enumerate(asks.items()):
                        records.append({
                            "timestamp": timestamp,
                            "symbol": symbol,
                            "update_id": update_id,
                            "side": "ask",
                            "level": i + 1,
                            "price": float(price),
                            "quantity": float(qty)
                        })
                    
                    if records:
                        batch_df = pl.DataFrame(records, schema=schema)
                        df = pl.concat([df, batch_df])
                    
                    self.snapshot_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            if first_snapshot is None or df.height == 0:
                return None
            
            symbol = first_snapshot.get("symbol", "UNKNOWN")
            start_time = datetime.fromtimestamp(first_snapshot.get("timestamp", time.time()))
            
            filename = f"{symbol}-{self.exchange}-orderbook_{start_time.strftime('%Y%m%d_%H%M%S')}_{self.current_file_index}.parquet"
            filepath = os.path.join(self.output_dir, filename)
            
            df.write_parquet(filepath, compression="zstd")
            
            logger.info(f"Saved {df.height} order book entries from {snapshot_count} snapshots to {filepath}")
            
            self._cleanup_old_files()
            
            if self.save_to_spaces and hasattr(self, 'spaces_client'):
                await asyncio.to_thread(self._upload_to_spaces, filepath, filename)
            
            return filepath
        except Exception as e:
            logger.error(f"Error saving snapshots to file: {e}")
            return None
    
    def _upload_to_spaces(self, local_filepath: str, filename: str):
        try:
            symbol = filename.split('-')[0].lower()
            
            spaces_key = f"snapshots/{symbol}/{filename}"
            
            with open(local_filepath, 'rb') as data:
                self.spaces_client.upload_fileobj(
                    data,
                    self.spaces_bucket,
                    spaces_key,
                    ExtraArgs={'ACL': 'private', 'ContentType': 'application/octet-stream'}
                )
            
            spaces_url = f"s3://{self.spaces_bucket}/{spaces_key}"
            logger.info(f"Uploaded {filename} to Digital Ocean Spaces: {spaces_url}")
            return spaces_url
        except Exception as e:
            logger.error(f"Failed to upload to Digital Ocean Spaces: {e}")
            return None