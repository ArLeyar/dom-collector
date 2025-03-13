"""Module for saving order book snapshots to Parquet."""

import os
import time
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd

from dom_collector.logger import logger


class OrderBookSnapshotSaver:
    """Class for saving order book snapshots to Parquet files."""
    
    def __init__(
        self,
        output_dir: str = "snapshots",
        max_snapshots_per_file: int = 3600,
        save_all_levels: bool = True,
    ):
        self.output_dir = output_dir
        self.max_snapshots_per_file = max_snapshots_per_file
        self.save_all_levels = save_all_levels
        self.snapshots = []
        self.snapshot_count = 0
        self.current_file_index = 0
        self.last_save_time = time.time()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Initialized OrderBookSnapshotSaver with output directory: {output_dir}")
        logger.info(f"Max snapshots per file: {max_snapshots_per_file}, save all levels: {save_all_levels}")
    
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
        if self.snapshot_count >= self.max_snapshots_per_file:
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
            
            # Clear snapshots and increment counters
            self.snapshots = []
            self.snapshot_count = 0
            self.current_file_index += 1
            self.last_save_time = time.time()
            
            return filepath
        except Exception as e:
            logger.error(f"Error saving snapshots to file: {e}")
            return None 