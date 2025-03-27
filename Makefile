.PHONY: collect-btc collect-eth collect-sol collect-all collect-btc-detached clean-snapshots help test

POETRY = poetry run
MODULE ?= dom_collector.cli
EXCHANGE = okx
INTERVAL = 1.0
SAVE_INTERVAL = 60
SAVE_TO_SPACES = true
RETENTION_HOURS = 24
MAX_DEPTH = 5000

help:
	@echo "DOM Collector Makefile"
	@echo ""
	@echo "Available commands:"
	@echo "  make collect-btc        - Collect BTC/USDT order book snapshots"
	@echo "  make collect-eth        - Collect ETH/USDT order book snapshots"
	@echo "  make collect-sol        - Collect SOL/USDT order book snapshots"
	@echo "  make collect-all        - Collect order book snapshots for all symbols"
	@echo "  make collect-btc-detached - Collect BTC/USDT order book snapshots in detached mode (for servers)"
	@echo "  make clean-snapshots    - Remove all snapshot files"
	@echo "  make test               - Run all tests"
	@echo ""
	@echo "Snapshots are taken every $(INTERVAL) seconds with new files created every $(SAVE_INTERVAL) seconds"
	@echo "Files are retained for $(RETENTION_HOURS) hours before being deleted"
	@echo "Save to Digital Ocean Spaces: $(SAVE_TO_SPACES)"
	@echo "Maximum depth: $(MAX_DEPTH) price levels per side"
	@echo ""
	@echo "Override defaults with:"
	@echo "  make collect-btc INTERVAL=0.5 SAVE_INTERVAL=1800 RETENTION_HOURS=48 SAVE_TO_SPACES=true MAX_DEPTH=10000"

collect-btc:
	mkdir -p snapshots
	$(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol BTC-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-eth:
	mkdir -p snapshots
	$(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol ETH-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-sol:
	mkdir -p snapshots
	$(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol SOL-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-all:
	mkdir -p snapshots
	$(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol BTC-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,) & \
	$(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol ETH-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,) & \
	$(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol SOL-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,)

collect-btc-detached:
	mkdir -p snapshots logs
	nohup $(POETRY) python -m $(MODULE) --exchange $(EXCHANGE) --symbol BTC-USDT --interval $(INTERVAL) --save-interval $(SAVE_INTERVAL) --retention-hours $(RETENTION_HOURS) --max-depth $(MAX_DEPTH) $(if $(filter true,$(SAVE_TO_SPACES)),--save-to-spaces,) > logs/collect-btc.log 2>&1 &
	@echo "BTC collection started in background. Check logs/collect-btc.log for output."
	@echo "Process ID: $$(pgrep -f "python -m $(MODULE) --exchange $(EXCHANGE) --symbol BTC-USDT")"

clean-snapshots:
	rm -rf snapshots/*.parquet 

test:
	poetry run pytest -v -s 