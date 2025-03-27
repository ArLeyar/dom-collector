import asyncio
import pytest
from dom_collector.okx_orderbook import OKXOrderBook

pytestmark = pytest.mark.asyncio

async def test_okx_orderbook_integration():
    print("\nStarting OKX orderbook test...")
    orderbook = OKXOrderBook(symbol="BTC-USDT", depth_limit=400, max_depth=400)
    
    async def check_orderbook():
        print("\nWaiting for orderbook data...")
        for i in range(20):  # Increased attempts
            ob = orderbook.get_full_order_book()
            print(f"\nAttempt {i+1}:")
            print(f"Connection state: {ob['connection_state']}")
            print(f"Total bids: {ob['total_bids']}")
            print(f"Total asks: {ob['total_asks']}")
            print(f"Last seq_id: {ob['last_seq_id']}")
            print(f"Last prev_seq_id: {ob['last_prev_seq_id']}")
            
            if ob["total_bids"] > 0 and ob["total_asks"] > 0:
                # Verify data integrity
                assert ob["last_seq_id"] > 0, f"Invalid seq_id: {ob['last_seq_id']}"
                assert ob["last_prev_seq_id"] >= -1, f"Invalid prev_seq_id: {ob['last_prev_seq_id']}"
                assert ob["connection_state"] == "connected", f"Invalid connection state: {ob['connection_state']}"
                assert len(ob["bids"]) > 0, "No bids received"
                assert len(ob["asks"]) > 0, "No asks received"
                
                # Verify price ordering
                bid_prices = list(ob["bids"].keys())
                ask_prices = list(ob["asks"].keys())
                assert bid_prices == sorted(bid_prices, reverse=True), "Bids not in descending order"
                assert ask_prices == sorted(ask_prices), "Asks not in ascending order"
                
                # Verify no zero quantities
                assert all(qty > 0 for qty in ob["bids"].values()), "Found zero or negative bid quantities"
                assert all(qty > 0 for qty in ob["asks"].values()), "Found zero or negative ask quantities"
                
                print("\nFirst 10 bids:")
                for price, qty in list(ob["bids"].items())[:10]:
                    print(f"Price: {price}, Quantity: {qty}")
                    
                print("\nFirst 10 asks:")
                for price, qty in list(ob["asks"].items())[:10]:
                    print(f"Price: {price}, Quantity: {qty}")
                
                # Wait a bit more to see if we get updates
                await asyncio.sleep(1)
                updated_ob = orderbook.get_full_order_book()
                if updated_ob["last_seq_id"] > ob["last_seq_id"]:
                    print("\nReceived orderbook updates!")
                    print(f"New seq_id: {updated_ob['last_seq_id']}")
                    print(f"Previous seq_id: {ob['last_seq_id']}")
                    
                return True
            await asyncio.sleep(0.5)
        return False
    
    print("\nStarting orderbook task...")
    task = asyncio.create_task(orderbook.start())
    await asyncio.sleep(2)  # Increased initial wait time
    
    print("\nChecking orderbook data...")
    has_data = await check_orderbook()
    assert has_data, "Orderbook did not receive initial data"
    
    print("\nTest completed successfully!")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass 