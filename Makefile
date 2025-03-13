.PHONY: collect-btc collect-eth collect-sol collect-all clean-snapshots help test

PYTHON = python
MODULE = src.dom_collector.cli
EXCHANGE = binance
SAVE_INTERVAL = 300.0

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
	@echo "All collection commands save snapshots every $(SAVE_INTERVAL) seconds"

collect-btc:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol btcusdt --auto-save-interval $(SAVE_INTERVAL)

collect-eth:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol ethusdt --auto-save-interval $(SAVE_INTERVAL)

collect-sol:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol solusdt --auto-save-interval $(SAVE_INTERVAL)

collect-all:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol btcusdt --auto-save-interval $(SAVE_INTERVAL) & \
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol ethusdt --auto-save-interval $(SAVE_INTERVAL) & \
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol solusdt --auto-save-interval $(SAVE_INTERVAL)

clean-snapshots:
	rm -rf snapshots/*.parquet 

test:
	poetry run pytest -v 