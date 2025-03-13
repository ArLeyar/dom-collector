"""Tests for verifying the correctness of order book snapshots."""

import os
import glob
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


class TestOrderBookSnapshots(unittest.TestCase):
    """Test case for verifying order book snapshots."""
    
    def setUp(self):
        """Set up the test case."""
        # Find all Parquet files in the snapshots directory
        # Look in both the current directory and the parent directory
        current_dir_snapshots = glob.glob("snapshots/*.parquet")
        parent_dir_snapshots = glob.glob("../snapshots/*.parquet")
        
        self.snapshot_files = current_dir_snapshots or parent_dir_snapshots
        self.assertTrue(self.snapshot_files, "No snapshot files found in the snapshots directory")
        
        # Load the first file for testing
        self.df = pd.read_parquet(self.snapshot_files[0])
        
    def test_file_naming_convention(self):
        """Test that snapshot files follow the correct naming convention."""
        for file_path in self.snapshot_files:
            file_name = os.path.basename(file_path)
            # Expected format: SYMBOL_orderbook_YYYYMMDD_HHMMSS_INDEX.parquet
            parts = file_name.split('_')
            self.assertEqual(len(parts), 5, f"File name {file_name} does not have the expected format")
            
            symbol = parts[0]
            self.assertTrue(symbol.isalpha() or all(c.isalpha() or c.isdigit() for c in symbol), 
                           f"Symbol {symbol} contains invalid characters")
            
            self.assertEqual(parts[1], "orderbook", f"File name {file_name} does not contain 'orderbook'")
            
            date_str = parts[2]
            time_str = parts[3]
            self.assertEqual(len(date_str), 8, f"Date string {date_str} is not 8 characters")
            self.assertEqual(len(time_str), 6, f"Time string {time_str} is not 6 characters")
            
            # Verify date and time can be parsed
            try:
                datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
            except ValueError:
                self.fail(f"Date and time {date_str}_{time_str} cannot be parsed")
            
            index_str = parts[4].split('.')[0]
            self.assertTrue(index_str.isdigit(), f"Index {index_str} is not a number")
    
    def test_required_columns(self):
        """Test that the snapshot data has all required columns."""
        required_columns = [
            'timestamp', 'symbol', 'update_id', 
            'side', 'level', 'price', 'quantity'
        ]
        for column in required_columns:
            self.assertIn(column, self.df.columns, f"Column {column} is missing")
    
    def test_data_types(self):
        """Test that the data types are correct."""
        self.assertEqual(self.df['timestamp'].dtype, 'float64', "timestamp should be float64")
        self.assertEqual(self.df['symbol'].dtype, 'object', "symbol should be object (string)")
        self.assertEqual(self.df['update_id'].dtype, 'int64', "update_id should be int64")
        self.assertEqual(self.df['side'].dtype, 'object', "side should be object (string)")
        self.assertEqual(self.df['level'].dtype, 'int64', "level should be int64")
        self.assertEqual(self.df['price'].dtype, 'float64', "price should be float64")
        self.assertEqual(self.df['quantity'].dtype, 'float64', "quantity should be float64")
    
    def test_side_values(self):
        """Test that side values are only 'bid' or 'ask'."""
        sides = self.df['side'].unique()
        self.assertEqual(len(sides), 2, "There should be exactly 2 unique side values")
        self.assertIn('bid', sides, "Side 'bid' is missing")
        self.assertIn('ask', sides, "Side 'ask' is missing")
    
    def test_level_values(self):
        """Test that level values are positive integers."""
        self.assertTrue((self.df['level'] > 0).all(), "All level values should be positive")
        
        # Check that levels are sequential for each update_id and side
        for update_id in self.df['update_id'].unique()[:5]:  # Test first 5 updates for performance
            for side in ['bid', 'ask']:
                levels = self.df[(self.df['update_id'] == update_id) & (self.df['side'] == side)]['level'].values
                self.assertEqual(list(levels), list(range(1, len(levels) + 1)), 
                                f"Levels for update_id {update_id}, side {side} are not sequential")
    
    def test_price_quantity_values(self):
        """Test that price and quantity values are non-negative."""
        self.assertTrue((self.df['price'] > 0).all(), "All price values should be positive")
        self.assertTrue((self.df['quantity'] >= 0).all(), "All quantity values should be non-negative")
    
    def test_update_id_ordering(self):
        """Test that update_ids are in ascending order."""
        update_ids = self.df['update_id'].unique()
        self.assertEqual(list(update_ids), sorted(update_ids), 
                        "Update IDs are not in ascending order")
    
    def test_snapshot_completeness(self):
        """Test that each snapshot has both bids and asks."""
        for update_id in self.df['update_id'].unique():
            snapshot = self.df[self.df['update_id'] == update_id]
            bids = snapshot[snapshot['side'] == 'bid']
            asks = snapshot[snapshot['side'] == 'ask']
            
            self.assertGreater(len(bids), 0, f"Snapshot {update_id} has no bids")
            self.assertGreater(len(asks), 0, f"Snapshot {update_id} has no asks")
    
    def test_price_ordering(self):
        """Test that prices are correctly ordered for bids and asks."""
        for update_id in self.df['update_id'].unique()[:5]:  # Test first 5 updates for performance
            # For bids, higher prices should have lower levels (descending order)
            bids = self.df[(self.df['update_id'] == update_id) & (self.df['side'] == 'bid')]
            bid_prices = bids.sort_values('level')['price'].values
            self.assertEqual(list(bid_prices), sorted(bid_prices, reverse=True), 
                            f"Bid prices for update_id {update_id} are not in descending order")
            
            # For asks, lower prices should have lower levels (ascending order)
            asks = self.df[(self.df['update_id'] == update_id) & (self.df['side'] == 'ask')]
            ask_prices = asks.sort_values('level')['price'].values
            self.assertEqual(list(ask_prices), sorted(ask_prices), 
                            f"Ask prices for update_id {update_id} are not in ascending order")
    
    def test_bid_ask_spread(self):
        """Test that the bid-ask spread is positive (no crossed book)."""
        for update_id in self.df['update_id'].unique():
            snapshot = self.df[self.df['update_id'] == update_id]
            
            # Get the highest bid and lowest ask
            highest_bid = snapshot[snapshot['side'] == 'bid']['price'].max()
            lowest_ask = snapshot[snapshot['side'] == 'ask']['price'].min()
            
            self.assertLess(highest_bid, lowest_ask, 
                           f"Bid-ask spread for update_id {update_id} is negative (crossed book)")
    
    def test_file_continuity(self):
        """Test that files have continuity in timestamps."""
        if len(self.snapshot_files) <= 1:
            self.skipTest("Need at least 2 files to test continuity")
        
        # Sort files by index
        sorted_files = sorted(self.snapshot_files, 
                             key=lambda x: int(os.path.basename(x).split('_')[4].split('.')[0]))
        
        for i in range(len(sorted_files) - 1):
            df1 = pd.read_parquet(sorted_files[i])
            df2 = pd.read_parquet(sorted_files[i + 1])
            
            last_timestamp_df1 = df1['timestamp'].max()
            first_timestamp_df2 = df2['timestamp'].min()
            
            # Files should be continuous or have a small gap
            self.assertLessEqual(last_timestamp_df1, first_timestamp_df2, 
                               "Timestamps between files are not in order")
            
            # Gap should not be too large (e.g., more than 5 minutes)
            gap = first_timestamp_df2 - last_timestamp_df1
            self.assertLessEqual(gap, 300, f"Gap between files {i} and {i+1} is too large: {gap} seconds")


if __name__ == '__main__':
    unittest.main() 