.PHONY: collect-btc collect-eth collect-sol collect-all clean-snapshots help test

PYTHON = python
MODULE = src.dom_collector.cli
EXCHANGE = binance
INTERVAL = 1.0
SNAPSHOTS_PER_FILE = 60

help:
	@echo "DOM Collector Makefile"
	@echo ""
	@echo "Available commands:"
	@echo "  make collect-btc        - Collect BTC/USDT order book snapshots"
	@echo "  make collect-eth        - Collect ETH/USDT order book snapshots"
	@echo "  make collect-sol        - Collect SOL/USDT order book snapshots"
	@echo "  make collect-all        - Collect order book snapshots for all symbols"
	@echo "  make clean-snapshots    - Remove all snapshot files"
	@echo "  make test               - Run all tests"
	@echo ""
	@echo "Snapshots are taken every $(INTERVAL) seconds with $(SNAPSHOTS_PER_FILE) snapshots per file"

collect-btc:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol btcusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE)

collect-eth:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol ethusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE)

collect-sol:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol solusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE)

collect-all:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol btcusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) & \
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol ethusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) & \
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol solusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE)

clean-snapshots:
	rm -rf snapshots/*.parquet 

test:
	poetry run pytest -v 