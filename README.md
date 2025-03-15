# DOM Collector

A tool for collecting depth of market (DOM) data from cryptocurrency exchanges.

## Features

- Real-time order book data collection from Binance
- Automatic saving of order book snapshots to Parquet files
- Configurable update intervals
- Comprehensive logging

## Installation

```bash
# Clone the repository
git clone https://github.com/ArLeyar/dom-collector.git
cd dom-collector

# Install with Poetry
poetry install
```

## Usage

### Command Line

```bash
# Display Binance order book for BTCUSDT with default settings
python -m src.dom_collector.cli binance

# Use a custom symbol and max depth
python -m src.dom_collector.cli binance --symbol ethusdt --max-depth 20000

# Set a custom snapshot interval and snapshots per file
python -m src.dom_collector.cli binance --interval 5.0 --snapshots-per-file 1800
```

### Using the Makefile

```bash
# Show available commands
make help

# Collect BTC/USDT order book snapshots
make collect-btc

# Collect ETH/USDT order book snapshots
make collect-eth

# Collect SOL/USDT order book snapshots
make collect-sol

# Collect order book snapshots for all symbols (BTC, ETH, SOL)
make collect-all

# Clean up all snapshot files
make clean-snapshots

# Run tests
make test
```

By default, snapshots are taken every 1 second with 300 snapshots per file (5 minutes of data).

## Testing

Run the test suite to verify that everything is working correctly:

```bash
# Run tests using the Makefile
make test

# Or run tests directly with pytest
poetry run pytest -v
```

## Data Analysis

The order book snapshots are saved in Parquet format, which is optimized for analytical workloads:

```python
import pandas as pd

# Load a Parquet file
df = pd.read_parquet("snapshots/BTCUSDT_orderbook_20230601_120000_0.parquet")

# Filter for bid orders only
bids = df[df["side"] == "bid"]

# Calculate average price at level 1
avg_bid_price = bids[bids["level"] == 1]["price"].mean()

# Analyze order book imbalance
df_pivot = df.pivot_table(
    index=["timestamp", "level"],
    columns="side",
    values="quantity"
).reset_index()
df_pivot["imbalance"] = df_pivot["bid"] / (df_pivot["bid"] + df_pivot["ask"])
```

## License

MIT 