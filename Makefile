.PHONY: collect-btc collect-eth collect-sol collect-all clean-snapshots help test

PYTHON = python
MODULE = src.dom_collector.cli
EXCHANGE = binance
INTERVAL = 1.0
SNAPSHOTS_PER_FILE = 60
SAVE_TO_SPACES = true

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
	@echo "Save to Digital Ocean Spaces: $(SAVE_TO_SPACES)"
	@echo ""
	@echo "Override defaults with:"
	@echo "  make collect-btc INTERVAL=0.5 SNAPSHOTS_PER_FILE=120 SAVE_TO_SPACES=true"

collect-btc:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol btcusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-eth:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol ethusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-sol:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol solusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-all:
	mkdir -p snapshots
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol btcusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,) & \
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol ethusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,) & \
	$(PYTHON) -m $(MODULE) $(EXCHANGE) --symbol solusdt --interval $(INTERVAL) --snapshots-per-file $(SNAPSHOTS_PER_FILE) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

clean-snapshots:
	rm -rf snapshots/*.parquet 

test:
	poetry run pytest -v 