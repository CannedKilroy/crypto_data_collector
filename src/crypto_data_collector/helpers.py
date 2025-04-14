# Helper functions
from pathlib import Path
import yaml
import redis
import ccxt.pro

class ConfigHandler:
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
        if self.config is None:
            self.load_config(self.config_path)
        if section is None:
            return self.config
        else:
            return self.config[section]

    def generate_config(self):
        """
        Returns a default config
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
        self.config = data

    def valid_config(self, config:dict):
        '''
        Verifies primarily configuration structure.
        Stream / exchange options need to be checked at runtime 

        :param e: 
        :return: 
        '''

        top_required_keys = {"exchanges", "consumers"}
        missing_keys = top_required_keys - set(config.keys())
        if missing_keys:
            raise ValueError(f"Missing Top Level Keys: {missing_keys}")
        
        if not isinstance(config.get("consumers"), dict):
            raise TypeError("Consumers value not a dict")
        if not isinstance(config.get("exchanges"), dict):
            raise TypeError("Exchanges value not a dict")

        for exchange_name, configs in config.get("exchanges").items():
            exchange_properties = configs["properties"]
            if not isinstance(exchange_properties, dict):
                raise TypeError(f"Exchange properties {exchange_properties} not a dict")

            for symbol, symbol_streams in configs["symbols"].items():
                if not isinstance(symbol_streams, dict):
                    raise TypeError(f"Symbol streams: {symbol_streams} not a dict")
                streams = symbol_streams['streams']
                if not isinstance(streams, dict):
                    raise TypeError(f"Streams: {streams} not a dict")
                
                for stream_name, stream_options in streams.items():
                    if not isinstance(stream_options, dict):
                        raise TypeError(f"Stream Options: {stream_options} not a dict")
                    options = stream_options["options"]
                    if not isinstance(options, dict):
                        raise TypeError(f"Options: {options} not a dict")
        return True
