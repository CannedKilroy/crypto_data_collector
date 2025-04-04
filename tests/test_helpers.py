from functools import reduce
from pathlib import Path
import yaml

class ConfigHandler:
    def __init__(self):
        self.config=None

    def default_config(self):
        data = {
            "consumers": {
                "archival_storage": {
                    "valid_streams": ["orderbook", "trades"],
                },
                "redis_db": {},
            },
            "exchanges": {
                "binance": {
                    "properties": {
                        "enableRateLimit": True,
                        "async_support": True,
                        "newUpdates": True,
                        "verbose": False,
                        "timeout": 10000,
                        "options": {},
                    },
                    "symbols": {
                        "BTC/USD:BTC": {
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}},
                            ],
                        },
                        "BTC/USDT:USDT": {
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}},
                            ],
                        },
                    },
                },
                "bitmex": {
                    "properties": {
                        "enableRateLimit": True,
                        "async_support": True,
                        "newUpdates": True,
                        "verbose": False,
                        "timeout": 10000,
                    },
                    "symbols": {
                        "BTC/USD:BTC": {
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}},
                            ],
                        },
                        "BTC/USDT:USDT": {
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}},
                            ],
                        },
                    },
                },
            },
        }
        self.config = data
        return data