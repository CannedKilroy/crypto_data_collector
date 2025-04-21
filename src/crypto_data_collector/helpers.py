from pathlib import Path
from typing import Union, Optional

import logging
import yaml
import ccxt.pro

from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)

def setup_logger(
    log_file_path: Optional[Union[str, Path]] = None,
    level: int = logging.INFO,
    console: bool = True,
    console_level: Optional[int] = None
    ) -> logging.Logger:
    """
    Configures the root logger to output to a rotating file (if path provided)
    and optionally to the console.

    :param log_file_path: Path or filename for the log file. If None, skip file logging.
    :param level: Logging level for file and console (if console_level not set).
    :param console: Whether to enable console (stdout) logging.
    :param console_level: Logging level for console handler (defaults to `level`).
    :return: The configured root logger instance.
    """
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s (in %(pathname)s:%(lineno)d)"
    )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)

    if log_file_path:
        log_file_path = Path(log_file_path)
        if not log_file_path.parent.exists():
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=str(log_file_path), maxBytes=10_240, backupCount=2
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level or level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    return root_logger


class ConfigHandler:
    def __init__(self, config_path=None):
        self.config=None
        self.config_path=config_path
        if self.config_path is not None:
            self._load_config(self.config_path)
        else:
            self.generate_config()

    def _load_config(self, config_path=None):
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
        Returns a default valid config
        """

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


def valid_config(config:dict):
    '''
    Verifies configuration structure.
    Invalid Stream / exchange options are ignored by ccxt
    Those are not checked
    Valid Config is shown in the docs

    :param config: Dict to be checked 
    :return: Bool whether config is valid
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
