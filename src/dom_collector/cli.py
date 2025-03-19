import argparse
import asyncio
import sys
import os
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
        "--depth", "-d", type=int, default=5000, help="Initial snapshot depth limit (default: 5000)"
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
        "--save-interval", "-si", type=int, default=3600,
        help="Time interval in seconds between creating new files (default: 3600, 1 hour)"
    )
    binance_parser.add_argument(
        "--save-to-spaces", action="store_true", 
        help="Save snapshots to Digital Ocean Spaces (requires DO_SPACES_* environment variables)"
    )
    binance_parser.add_argument(
        "--retention-hours", type=int, default=24,
        help="How long to keep files before deleting them (in hours, default: 24)"
    )
    
    return parser.parse_args()


async def run_binance_orderbook(args):
    symbol = args.symbol
    depth_limit = args.depth
    max_depth = args.max_depth
    interval = args.interval
    save_interval_seconds = args.save_interval
    save_to_spaces = args.save_to_spaces
    retention_hours = args.retention_hours
    
    if save_to_spaces:
        if not os.getenv("DO_SPACES_BUCKET"):
            logger.error("DO_SPACES_BUCKET environment variable is required when --save-to-spaces is used")
            sys.exit(1)
    
    os.makedirs(args.parquet_dir, exist_ok=True)
    
    logger.info(f"Starting order book manager for {symbol.upper()}")
    logger.info(f"Using max depth of {max_depth} price levels per side")
    logger.info(f"Saving snapshots every {interval} seconds")
    logger.info(f"Creating new files every {save_interval_seconds} seconds (approx. {save_interval_seconds / 3600:.1f} hours of data)")
    logger.info(f"Files will be kept for {retention_hours} hours before being deleted")
    
    if save_to_spaces:
        logger.info(f"Saving snapshots to Digital Ocean Spaces bucket: {os.getenv('DO_SPACES_BUCKET')}")
    
    order_book = BinanceOrderBook(
        symbol=symbol, 
        depth_limit=depth_limit, 
        max_depth=max_depth
    )
    
    snapshot_saver = OrderBookSnapshotSaver(
        output_dir=args.parquet_dir,
        save_interval_seconds=save_interval_seconds,
        save_to_spaces=save_to_spaces,
        retention_hours=retention_hours,
    )
    
    await snapshot_saver.start()
    
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        logger.warning("Shutdown signal received, initiating graceful shutdown...")
        shutdown_event.set()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
    
    order_book_task = asyncio.create_task(order_book.start())
    
    try:
        while not shutdown_event.is_set():
            try:
                current_book = order_book.get_full_order_book()
                if current_book["bids"] and current_book["asks"]:
                    await snapshot_saver.add_snapshot(current_book)
                    logger.debug(f"Saved snapshot for {current_book['symbol']} (Update ID: {current_book['lastUpdateId']})")
            except Exception as e:
                logger.error(f"Error saving snapshot: {e}")
            
            await asyncio.sleep(interval)
                
    except asyncio.CancelledError:
        logger.debug("Main loop cancelled")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        logger.info("Shutting down...")
        
        await snapshot_saver.stop()
        
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