"""Command-line interface for DOM Collector."""

import argparse
import asyncio
import json
import sys
import os
import time

from dom_collector.binance_orderbook import BinanceOrderBook
from dom_collector.snapshot_saver import OrderBookSnapshotSaver
from dom_collector.logger import logger


def parse_args():
    parser = argparse.ArgumentParser(description="DOM Collector CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    binance_parser = subparsers.add_parser("binance", help="Manage Binance order book")
    binance_parser.add_argument(
        "--symbol", "-s", default="btcusdt", help="Trading symbol (default: btcusdt)"
    )
    binance_parser.add_argument(
        "--depth", "-d", type=int, default=5000, help="Depth limit (default: 5000)"
    )
    binance_parser.add_argument(
        "--display", "-n", type=int, default=10, help="Number of levels to display (default: 10)"
    )
    binance_parser.add_argument(
        "--save", "-o", help="Save full order book to file (specify filename)"
    )
    binance_parser.add_argument(
        "--interval", "-i", type=float, default=1.0, help="Update interval in seconds (default: 1.0)"
    )
    binance_parser.add_argument(
        "--parquet-dir", default="snapshots", help="Directory to save Parquet files (default: snapshots)"
    )
    binance_parser.add_argument(
        "--save-all-updates", action="store_true", 
        help="Save a snapshot for every order book update received"
    )
    binance_parser.add_argument(
        "--auto-save-interval", type=float, default=300.0,
        help="Interval in seconds to automatically save snapshots to disk (default: 300.0)"
    )
    
    return parser.parse_args()


class OrderBookManager:
    def __init__(self, order_book, snapshot_saver=None, save_all_updates=False, auto_save_interval=60.0):
        self.order_book = order_book
        self.snapshot_saver = snapshot_saver
        self.save_all_updates = save_all_updates
        self.last_update_id = 0
        self.auto_save_interval = auto_save_interval
        self.last_auto_save = time.time()
        logger.info(f"Initialized OrderBookManager with save_all_updates={save_all_updates}")
        logger.info(f"Auto-saving snapshots every {auto_save_interval} seconds")
        
    async def process_updates(self):
        while True:
            try:
                await asyncio.sleep(0.01)
                
                current_time = time.time()
                
                current_update_id = self.order_book.last_update_id
                if current_update_id > self.last_update_id and self.snapshot_saver and self.save_all_updates:
                    full_book = self.order_book.get_full_order_book()
                    if full_book["bids"] and full_book["asks"]:
                        self.snapshot_saver.add_snapshot(full_book)
                        logger.debug(f"Saved snapshot for update ID: {current_update_id}")
                    self.last_update_id = current_update_id
                
                if self.snapshot_saver and (current_time - self.last_auto_save) >= self.auto_save_interval:
                    logger.info(f"Auto-saving snapshots to disk (every {self.auto_save_interval} seconds)...")
                    self.snapshot_saver.save_to_file()
                    self.last_auto_save = current_time
                    
            except asyncio.CancelledError:
                break


async def run_binance_orderbook(args):
    symbol = args.symbol
    depth_limit = args.depth
    display_levels = args.display
    save_file = args.save
    interval = args.interval
    auto_save_interval = args.auto_save_interval
    
    logger.info(f"Starting order book manager for {symbol.upper()}")
    order_book = BinanceOrderBook(symbol, depth_limit)
    
    snapshot_saver = OrderBookSnapshotSaver(
        output_dir=args.parquet_dir,
        max_snapshots_per_file=3600,
        save_all_levels=True
    )
    logger.info(f"Saving order book snapshots to Parquet files in directory: {args.parquet_dir}")
    logger.info(f"Auto-saving to disk every {auto_save_interval} seconds")
    
    order_book_task = asyncio.create_task(order_book.start())
    
    manager = OrderBookManager(
        order_book, 
        snapshot_saver, 
        args.save_all_updates,
        auto_save_interval
    )
    
    # Always create the update processor task to ensure regular saving
    update_processor_task = asyncio.create_task(manager.process_updates())
    
    last_snapshot_time = 0
    
    try:
        while True:
            await asyncio.sleep(interval)
            current_book = order_book.get_order_book(display_levels)
            
            current_time = asyncio.get_event_loop().time()
            if not args.save_all_updates and (current_time - last_snapshot_time) >= interval:
                full_book = order_book.get_full_order_book()
                if full_book["bids"] and full_book["asks"]:
                    snapshot_saver.add_snapshot(full_book)
                    last_snapshot_time = current_time
            
            logger.info(f"Order Book for {current_book['symbol']} (Update ID: {current_book['lastUpdateId']})")
            logger.info(f"Total Bids: {current_book['total_bids']}, Total Asks: {current_book['total_asks']}")
            
            if save_file and current_book["total_bids"] > 0 and current_book["total_asks"] > 0:
                full_book = order_book.get_full_order_book()
                with open(save_file, 'w') as f:
                    json.dump(full_book, f, indent=2)
                logger.info(f"Full order book saved to {save_file}")
                
    except asyncio.CancelledError:
        logger.debug("Main loop cancelled")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        if update_processor_task:
            update_processor_task.cancel()
        if order_book_task:
            order_book_task.cancel()


def main():
    args = parse_args()
    
    if args.command == "binance":
        try:
            logger.info("Starting DOM Collector CLI with Binance order book")
            asyncio.run(run_binance_orderbook(args))
        except KeyboardInterrupt:
            logger.warning("Program interrupted by user. Exiting...")
        except Exception as e:
            logger.error(f"Error: {e}")
    else:
        logger.error("Please specify a command. Use --help for more information.")
        sys.exit(1)


if __name__ == "__main__":
    main() 