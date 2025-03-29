# Helper functions
from pathlib import Path
import yaml
import redis


def redis_connector():
    conn = redis.Redis(
    db=0,
    host = "localhost",
    decode_responses=True,
    retry_on_timeout=True)
    
    if conn.ping() is not True:
        raise Exception("You done goofed")
    else:
        conn.flushdb()
    return conn


class ConfigHandler:
    """
    Loads config on initialization
    Can also reload config and call again
    """
    def __init__(self, config_path=None):
        self.config=None
        self.config_path=config_path
        if self.config_path is not None:
            self.load_config(self.config_path)
        else:
            self.generate_config()

    def load_config(self, config_path=None):
        if config_path is None:
            current_script_path = Path(__file__).resolve()
            project_root = current_script_path.parent.parent
            config_path = project_root / 'config' / 'config.yaml'
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            self.config = config

    def get_config(self, section=None):
        if self.config == None:
            self.load_config(self.config_path)
        if section is None:
            return self.config
        else:
            return self.config[section]

    def generate_config(self):
        """
        Generates a default config
        """
        #current_script_path = Path(__file__).resolve()
        #project_root = current_script_path.parent.parent
        #config_path = project_root / 'config' / 'config.yaml'
        # All overrides are held in here
        data = {
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
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}}
                            ]
                        },
                        "BTC/USDT:USDT": {
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}}
                            ]
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
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}}
                            ]
                        },
                        "BTC/USDT:USDT": {
                            "streams": [
                                {"watchOHLCV": {"options": {}}},
                                {"watchTicker": {"options": {}}},
                                {"watchTrades": {"options": {}}},
                                {"watchOrderBook": {"options": {}}}
                            ]
                        }
                    }
                }
            }
        }

        #with open(config_path, 'w') as file:
        #    yaml.dump(data=data, stream=file, default_flow_style=False)
        self.config = data

def load_test():
    # Adds many symbols and exchanges to see how my script handles it
    pass


if __name__ == "__main__":
    versions()