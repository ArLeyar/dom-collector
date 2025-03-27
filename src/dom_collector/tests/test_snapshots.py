import os
import polars as pl
from typing import List

def get_snapshot_files() -> List[str]:
    """Get all parquet files from snapshots directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    return [f for f in os.listdir(snapshot_dir) if f.endswith('.parquet')]

def read_snapshot(filepath: str) -> pl.DataFrame:
    """Read a parquet file and return as DataFrame."""
    return pl.read_parquet(filepath)

def verify_orderbook_structure(df: pl.DataFrame) -> bool:
    """Verify that the orderbook data structure is correct."""
    required_columns = {"timestamp", "symbol", "update_id", "side", "level", "price", "quantity"}
    if not required_columns.issubset(df.columns):
        return False
    
    # Check data types
    if not (df["price"].dtype == pl.Float64 and df["quantity"].dtype == pl.Float64):
        return False
    
    # Check for negative prices or quantities
    if df["price"].min() < 0 or df["quantity"].min() < 0:
        return False
    
    # Check that levels are sequential for each timestamp and update_id
    for side in ["bid", "ask"]:
        side_df = df.filter(pl.col("side") == side)
        # Get unique combinations of timestamp and update_id
        unique_combos = side_df.select(["timestamp", "update_id"]).unique()
        
        # Check each combination
        for row in unique_combos.iter_rows():
            timestamp, update_id = row
            group_df = side_df.filter(
                (pl.col("timestamp") == timestamp) & 
                (pl.col("update_id") == update_id)
            )
            levels = group_df["level"].sort()
            if not (levels == pl.Series(range(1, len(levels) + 1))).all():
                return False
    
    return True

def verify_orderbook_depth(df: pl.DataFrame) -> bool:
    """Verify that the orderbook has the expected depth."""
    # Get the maximum level for each side
    max_bid_level = df.filter(pl.col("side") == "bid")["level"].max()
    max_ask_level = df.filter(pl.col("side") == "ask")["level"].max()
    
    # Check if both sides have the same depth
    if max_bid_level != max_ask_level:
        return False
    
    # Check if the depth is 400 (OKX's default depth)
    if max_bid_level != 400:
        return False
    
    return True

def check_price_ordering(df: pl.DataFrame) -> bool:
    """Verify that bids are in descending order and asks in ascending order."""
    # Get unique combinations of timestamp and update_id
    unique_combos = df.select(["timestamp", "update_id"]).unique()
    
    # Check each combination
    for row in unique_combos.iter_rows():
        timestamp, update_id = row
        group_df = df.filter(
            (pl.col("timestamp") == timestamp) & 
            (pl.col("update_id") == update_id)
        )
        bids = group_df.filter(pl.col("side") == "bid")
        asks = group_df.filter(pl.col("side") == "ask")
        
        if not bids["price"].is_sorted(descending=True):
            return False
        if not asks["price"].is_sorted():
            return False
    
    return True

def check_timestamp_continuity(df: pl.DataFrame) -> bool:
    """Check for gaps in timestamps."""
    timestamps = df["timestamp"].unique().sort()
    if len(timestamps) < 2:
        return True
    
    time_diffs = timestamps.diff()
    max_gap = time_diffs.max()
    
    # Allow for small gaps (less than 2 seconds)
    return max_gap < 2.0

def test_snapshot_files_exist():
    """Test that snapshot files exist."""
    files = get_snapshot_files()
    assert len(files) > 0, "No snapshot files found"
    print(f"\nFound {len(files)} snapshot files")

def test_snapshot_structure():
    """Test the structure of snapshot data."""
    files = get_snapshot_files()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    
    for file in files:
        filepath = os.path.join(snapshot_dir, file)
        print(f"\nTesting structure for file: {file}")
        
        df = read_snapshot(filepath)
        print(f"Total records: {len(df)}")
        print(f"Unique timestamps: {df['timestamp'].n_unique()}")
        print(f"Unique symbols: {df['symbol'].unique()}")
        
        assert verify_orderbook_structure(df), f"Invalid orderbook structure in {file}"
        print("✓ Orderbook structure verified")

def test_snapshot_depth():
    """Test the depth of snapshot data."""
    files = get_snapshot_files()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    
    for file in files:
        filepath = os.path.join(snapshot_dir, file)
        print(f"\nTesting depth for file: {file}")
        
        df = read_snapshot(filepath)
        max_bid_level = df.filter(pl.col("side") == "bid")["level"].max()
        max_ask_level = df.filter(pl.col("side") == "ask")["level"].max()
        
        print(f"Max bid level: {max_bid_level}")
        print(f"Max ask level: {max_ask_level}")
        
        assert verify_orderbook_depth(df), f"Invalid orderbook depth in {file}"
        print("✓ Orderbook depth verified")

def test_snapshot_price_ordering():
    """Test the price ordering in snapshot data."""
    files = get_snapshot_files()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    
    for file in files:
        filepath = os.path.join(snapshot_dir, file)
        print(f"\nTesting price ordering for file: {file}")
        
        df = read_snapshot(filepath)
        
        # Print price ranges
        print("\nPrice ranges:")
        for side in ["bid", "ask"]:
            side_df = df.filter(pl.col("side") == side)
            print(f"{side}: {side_df['price'].min():.2f} - {side_df['price'].max():.2f}")
        
        assert check_price_ordering(df), f"Invalid price ordering in {file}"
        print("✓ Price ordering verified")

def test_snapshot_timestamp_continuity():
    """Test the timestamp continuity in snapshot data."""
    files = get_snapshot_files()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    
    for file in files:
        filepath = os.path.join(snapshot_dir, file)
        print(f"\nTesting timestamp continuity for file: {file}")
        
        df = read_snapshot(filepath)
        timestamps = df["timestamp"].unique().sort()
        
        if len(timestamps) >= 2:
            time_diffs = timestamps.diff()
            max_gap = time_diffs.max()
            print(f"Max time gap: {max_gap:.2f} seconds")
        
        assert check_timestamp_continuity(df), f"Large gaps in timestamps in {file}"
        print("✓ Timestamp continuity verified")

def test_snapshot_quantity_ranges():
    """Test the quantity ranges in snapshot data."""
    files = get_snapshot_files()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    
    for file in files:
        filepath = os.path.join(snapshot_dir, file)
        print(f"\nTesting quantity ranges for file: {file}")
        
        df = read_snapshot(filepath)
        
        print("\nQuantity ranges:")
        for side in ["bid", "ask"]:
            side_df = df.filter(pl.col("side") == side)
            min_qty = side_df["quantity"].min()
            max_qty = side_df["quantity"].max()
            print(f"{side}: {min_qty:.2f} - {max_qty:.2f}")
            
            # Check for negative quantities
            assert min_qty >= 0, f"Negative quantity found in {side} side of {file}"

def test_snapshot_sequence():
    """Test that snapshots are properly sequenced."""
    files = get_snapshot_files()
    if len(files) < 2:
        return
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    snapshot_dir = os.path.join(current_dir, "..", "..", "..", "snapshots")
    
    # Sort files by timestamp
    files.sort()
    
    # Check consecutive files for gaps
    for i in range(len(files) - 1):
        current_file = files[i]
        next_file = files[i + 1]
        
        current_df = read_snapshot(os.path.join(snapshot_dir, current_file))
        next_df = read_snapshot(os.path.join(snapshot_dir, next_file))
        
        current_time = current_df["timestamp"].max()
        next_time = next_df["timestamp"].min()
        
        time_diff = next_time - current_time
        print(f"\nTime gap between {current_file} and {next_file}: {time_diff:.2f} seconds")
        
        # Allow for small gaps (less than 2 seconds)
        assert time_diff < 2.0, f"Large gap ({time_diff:.2f}s) between snapshots" 