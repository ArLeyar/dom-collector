"""Module for saving order book snapshots to Parquet."""

import os
import time
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
import boto3
from dotenv import load_dotenv

from dom_collector.logger import logger

load_dotenv()


class OrderBookSnapshotSaver:
    """Class for saving order book snapshots to Parquet files."""
    
    def __init__(
        self,
        output_dir: str = "snapshots",
        snapshots_per_file: int = 3600,
        save_to_spaces: bool = False,
    ):
        self.output_dir = output_dir
        self.snapshots_per_file = snapshots_per_file
        self.snapshots = []
        self.snapshot_count = 0
        self.current_file_index = 0
        self.save_to_spaces = save_to_spaces
        self.spaces_bucket = None
        
        if self.save_to_spaces:
            self._init_spaces_client()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Initialized OrderBookSnapshotSaver with output directory: {output_dir}")
        logger.info(f"Snapshots per file: {snapshots_per_file}")
        if self.save_to_spaces and hasattr(self, 'spaces_client'):
            logger.info(f"Digital Ocean Spaces enabled with bucket: {self.spaces_bucket}")
    
    def _init_spaces_client(self):
        """Initialize the Digital Ocean Spaces client."""
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
        """Get the number of pending snapshots."""
        return self.snapshot_count
    
    def add_snapshot(self, order_book: Dict[str, Any], timestamp: Optional[float] = None):
        """
        Add an order book snapshot.
        
        Args:
            order_book: The order book data
            timestamp: Optional timestamp (if not provided, current time is used)
        """
        if timestamp is None:
            timestamp = time.time()
            
        # Extract symbol and update ID
        symbol = order_book.get("symbol", "UNKNOWN")
        update_id = order_book.get("lastUpdateId", 0)
        
        # Process bids and asks
        bids = order_book.get("bids", {})
        asks = order_book.get("asks", {})
        
        # Sort bids (descending) and asks (ascending)
        sorted_bids = sorted(bids.items(), key=lambda x: float(x[0]), reverse=True)
        sorted_asks = sorted(asks.items(), key=lambda x: float(x[0]))
        
        # Create bid and ask records
        bid_records = []
        for i, (price, qty) in enumerate(sorted_bids):
            bid_records.append({
                "timestamp": timestamp,
                "symbol": symbol,
                "update_id": update_id,
                "side": "bid",
                "level": i + 1,
                "price": float(price),
                "quantity": float(qty)
            })
            
        ask_records = []
        for i, (price, qty) in enumerate(sorted_asks):
            ask_records.append({
                "timestamp": timestamp,
                "symbol": symbol,
                "update_id": update_id,
                "side": "ask",
                "level": i + 1,
                "price": float(price),
                "quantity": float(qty)
            })
            
        # Add all records to snapshots
        self.snapshots.extend(bid_records + ask_records)
        self.snapshot_count += 1
        
        logger.debug(f"Added snapshot for {symbol} with update_id {update_id}")
        
        # Save to file if we've reached the maximum number of snapshots per file
        if self.snapshot_count >= self.snapshots_per_file:
            self.save_to_file()
    
    def save_to_file(self):
        """Save the current snapshots to a Parquet file."""
        if not self.snapshots:
            logger.debug("No snapshots to save")
            return
            
        try:
            # Create DataFrame from snapshots
            df = pd.DataFrame(self.snapshots)
            
            # Generate filename based on timestamp and symbol
            first_record = self.snapshots[0]
            symbol = first_record["symbol"]
            start_time = datetime.fromtimestamp(first_record["timestamp"])
            
            filename = f"{symbol}_orderbook_{start_time.strftime('%Y%m%d_%H%M%S')}_{self.current_file_index}.parquet"
            filepath = os.path.join(self.output_dir, filename)
            
            # Save to Parquet
            df.to_parquet(filepath, engine="pyarrow", compression="snappy")
            
            logger.info(f"Saved {len(self.snapshots)} order book entries to {filepath}")
            
            # Upload to Spaces if enabled
            if self.save_to_spaces and hasattr(self, 'spaces_client'):
                self._upload_to_spaces(filepath, filename)
            
            # Clear snapshots and increment counters
            self.snapshots = []
            self.snapshot_count = 0
            self.current_file_index += 1
            
            return filepath
        except Exception as e:
            logger.error(f"Error saving snapshots to file: {e}")
            return None
    
    def _upload_to_spaces(self, local_filepath: str, filename: str):
        """Upload a file to Digital Ocean Spaces."""
        try:
            # Extract symbol from filename (format: SYMBOL_orderbook_TIMESTAMP_INDEX.parquet)
            symbol = filename.split('_')[0].lower()
            
            # Create key with snapshots/symbol as folder structure
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