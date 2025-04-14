import pytest

@pytest.fixture
def valid_config():
    return {
    "consumers": {
        "archival_storage": {
            "valid_streams": ["orderbook", "trades"]
        },
        "redis_db": None
    },
    
    "exchanges": {
        "binance": {
            "properties": {
                "enableRateLimit": True,
                "async_support": True,
                "newUpdates": True,
                "verbose": False,
                "timeout": 10000,
                "options": {}
            },
            "symbols": {
                "BTC/USD:BTC": {
                    "streams": {
                        "watchOHLCV": {"options": {}},
                        "watchTicker": {"options": {}},
                        "watchTrades": {"options": {}},
                        "watchOrderBook": {"options": {}}
                        }
                        },
                "BTC/USDT:USDT": {
                    "streams": {
                        "watchOHLCV": {"options": {}},
                        "watchTicker": {"options": {}},
                        "watchTrades": {"options": {}},
                        "watchOrderBook": {"options": {}}
                        }
                        }
                        }
                        },
        "bitmex": {
            "properties": {
                "enableRateLimit": True,
                "async_support": True,
                "newUpdates": True,
                "verbose": False,
                "timeout": 10000
            },
            "symbols": {
                "BTC/USD:BTC": {
                    "streams": {
                        "watchOHLCV": {"options": {}},
                        "watchTicker": {"options": {}},
                        "watchTrades": {"options": {}},
                        "watchOrderBook": {"options": {}}
                        }
                        },
                "BTC/USDT:USDT": {
                    "streams": {
                        "watchOHLCV": {"options": {}},
                        "watchTicker": {"options": {}},
                        "watchTrades": {"options": {}},
                        "watchOrderBook": {"options": {}}
                        }
                        }
                        }
                        }
                        }
                        }