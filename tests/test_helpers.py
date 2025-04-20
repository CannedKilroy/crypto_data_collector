import pytest

from src.crypto_data_collector.helpers import ConfigHandler
from src.crypto_data_collector.producer import initialize_exchanges



valid_config = {
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