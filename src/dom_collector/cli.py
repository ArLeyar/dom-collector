import argparse
import asyncio
import sys
import os
import time
import signal
from dotenv import load_dotenv

from dom_collector.binance_orderbook import BinanceOrderBook
from dom_collector.snapshot_saver import OrderBookSnapshotSaver
from dom_collector.logger import logger

load_dotenv()


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
        "--max-depth", "-m", type=int, default=10000, 
        help="Maximum number of price levels to store per side (bids/asks) (default: 10000)"
    )
    binance_parser.add_argument(
        "--interval", "-i", type=float, default=1.0, help="Update interval in seconds (default: 1.0)"
    )
    binance_parser.add_argument(
        "--parquet-dir", default="snapshots", help="Directory to save Parquet files (default: snapshots)"
    )
    binance_parser.add_argument(
        "--snapshots-per-file", type=int, default=3600,
        help="Number of snapshots to store in each file (default: 3600, ~1 hour at 1 snapshot/second)"
    )
    binance_parser.add_argument(
        "--save-to-spaces", action="store_true", 
        help="Save snapshots to Digital Ocean Spaces (requires DO_SPACES_* environment variables)"
    )
    
    return parser.parse_args()


async def run_binance_orderbook(args):
    symbol = args.symbol
    depth_limit = args.depth
    max_depth = args.max_depth
    interval = args.interval
    snapshots_per_file = args.snapshots_per_file
    save_to_spaces = args.save_to_spaces
    
    if save_to_spaces:
        # Check if DO_SPACES_BUCKET is set
        if not os.getenv("DO_SPACES_BUCKET"):
            logger.error("DO_SPACES_BUCKET environment variable is required when --save-to-spaces is used")
            sys.exit(1)
    
    os.makedirs(args.parquet_dir, exist_ok=True)
    
    logger.info(f"Starting order book manager for {symbol.upper()}")
    logger.info(f"Using max depth of {max_depth} price levels per side")
    logger.info(f"Saving snapshots every {interval} seconds")
    logger.info(f"Snapshots per file: {snapshots_per_file} (approx. {snapshots_per_file * interval / 3600:.1f} hours of data)")
    
    if save_to_spaces:
        logger.info(f"Saving snapshots to Digital Ocean Spaces bucket: {os.getenv('DO_SPACES_BUCKET')}")
    
    order_book = BinanceOrderBook(
        symbol=symbol, 
        depth_limit=depth_limit, 
        max_depth=max_depth
    )
    
    snapshot_saver = OrderBookSnapshotSaver(
        output_dir=args.parquet_dir,
        snapshots_per_file=snapshots_per_file,
        save_to_spaces=save_to_spaces,
    )
    
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        logger.warning("Shutdown signal received, initiating graceful shutdown...")
        shutdown_event.set()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
    
    order_book_task = asyncio.create_task(order_book.start())
    
    last_snapshot_time = 0
    last_status_time = 0
    last_connection_state = "disconnected"
    last_state_change = time.time()
    
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(0.1)
            
            try:
                current_time = time.time()
                
                if hasattr(order_book, "connection_state"):
                    current_state = order_book.connection_state
                    if current_state != last_connection_state:
                        state_duration = current_time - last_state_change
                        logger.info(f"Connection state changed: {last_connection_state} → {current_state} (after {state_duration:.1f}s)")
                        last_connection_state = current_state
                        last_state_change = current_time
                
                if (current_time - last_snapshot_time) >= interval:
                    current_book = order_book.get_full_order_book()
                    if current_book["bids"] and current_book["asks"]:
                        snapshot_saver.add_snapshot(current_book)
                        last_snapshot_time = current_time
                
                if (current_time - last_status_time) >= 5.0:  # Hardcoded 5 second status interval
                    current_book = order_book.get_full_order_book()
                    connection_state = current_book.get("connection_state", "unknown")
                    logger.info(f"Order Book for {current_book['symbol']} (Update ID: {current_book['lastUpdateId']}, State: {connection_state})")
                    logger.info(f"Total Bids: {current_book['total_bids']}/{max_depth}, Total Asks: {current_book['total_asks']}/{max_depth}")
                    last_status_time = current_time
            
            except Exception as e:
                logger.error(f"Error processing order book data: {e}")
                await asyncio.sleep(1)
                
    except asyncio.CancelledError:
        logger.debug("Main loop cancelled")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        logger.info("Shutting down...")
        
        if snapshot_saver and snapshot_saver.pending_snapshots > 0:
            logger.info(f"Saving {snapshot_saver.pending_snapshots} pending snapshots before exit")
            try:
                snapshot_saver.save_to_file()
            except Exception as e:
                logger.error(f"Error saving snapshots during shutdown: {e}")
        
        if order_book_task:
            order_book_task.cancel()
            try:
                await order_book_task
            except asyncio.CancelledError:
                pass
                
        logger.info("Shutdown complete")


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
            sys.exit(1)
    else:
        logger.error("Please specify a command. Use --help for more information.")
        sys.exit(1)


if __name__ == "__main__":
    main() 