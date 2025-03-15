"""Tests for the Binance order book module."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dom_collector.binance_orderbook import BinanceOrderBook


def test_binance_orderbook_init():
    """Test BinanceOrderBook initialization."""
    order_book = BinanceOrderBook("btcusdt")
    assert order_book.symbol == "btcusdt"
    assert order_book.ws_url == "wss://stream.binance.com:9443/ws/btcusdt@depth"
    assert order_book.order_book == {"bids": {}, "asks": {}}
    assert order_book.last_update_id == 0


def test_apply_snapshot():
    """Test applying a snapshot to the order book."""
    order_book = BinanceOrderBook()
    
    snapshot = {
        "lastUpdateId": 12345,
        "bids": [["0.01", "100"], ["0.009", "200"]],
        "asks": [["0.011", "300"], ["0.012", "400"]]
    }
    
    order_book.apply_snapshot(snapshot)
    
    assert order_book.last_update_id == 12345
    assert order_book.order_book["bids"] == {0.01: 100.0, 0.009: 200.0}
    assert order_book.order_book["asks"] == {0.011: 300.0, 0.012: 400.0}


def test_process_event_valid():
    """Test processing a valid event."""
    order_book = BinanceOrderBook()
    order_book.last_update_id = 12345
    
    event = {
        "e": "depthUpdate",
        "E": 123456789,
        "s": "BNBBTC",
        "U": 12346,
        "u": 12350,
        "b": [["0.01", "10"], ["0.02", "0"]],
        "a": [["0.03", "5"]]
    }
    
    # Set initial state
    order_book.order_book = {
        "bids": {0.01: 5.0, 0.02: 3.0},
        "asks": {0.03: 2.0, 0.04: 1.0}
    }
    
    result = order_book.process_event(event)
    
    assert result is True
    assert order_book.last_update_id == 12350
    assert order_book.order_book["bids"] == {0.01: 10.0}  # Updated
    assert 0.02 not in order_book.order_book["bids"]  # Removed (qty=0)
    assert order_book.order_book["asks"] == {0.03: 5.0, 0.04: 1.0}  # Updated


def test_process_event_invalid():
    """Test processing an invalid event."""
    order_book = BinanceOrderBook()
    order_book.last_update_id = 12345
    
    # Event with u < last_update_id
    event1 = {
        "e": "depthUpdate",
        "E": 123456789,
        "s": "BNBBTC",
        "U": 12340,
        "u": 12344,
        "b": [],
        "a": []
    }
    
    # Event with U > last_update_id + 1
    event2 = {
        "e": "depthUpdate",
        "E": 123456789,
        "s": "BNBBTC",
        "U": 12347,
        "u": 12350,
        "b": [],
        "a": []
    }
    
    assert order_book.process_event(event1) is False
    assert order_book.process_event(event2) is False 