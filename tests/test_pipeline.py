import pytest

from src.crypto_data_collector.helpers import ConfigHandler
from src.crypto_data_collector.producer import initialize_exchanges

from tests.test_helpers import valid_config
def test_true():
    assert True

def test_config_structure():
    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    assert config_handler.valid_config(config) is True

def test_missing_top_level_keys():
    config_handler = ConfigHandler()
    config = {
        "consumers":{},
        # Missing exchanges
        }
    with pytest.raises(ValueError, match = "Missing Top Level Keys") as excinfo:
        config_handler.valid_config(config)

def test_exchanges_not_dict():
    config_handler = ConfigHandler()
    config = {
        "consumers":{},
        "exchanges":[
            {"binance":{"properties":{}}}
            ]
        }
    with pytest.raises(TypeError, match = "Exchanges value not a dict") as excinfo:
        config_handler.valid_config(config)

def test_consumers_not_dict():
    config_handler = ConfigHandler()
    config = {
        "consumers":[],
        "exchanges":{
            "binance":{"properties":{}}
            }
        }
    with pytest.raises(TypeError, match = "Consumers value not a dict") as excinfo:
        config_handler.valid_config(config)

# Invalid exchange
async def test_initialize_exchanges_invalid_exch():
    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    config["exchanges"]["invalid_exhchange"] = {}
    with pytest.raises(AttributeError, match = "Invalid exchange") as excinfo:
        exchanges = await initialize_exchanges(config)

# Invalid symbol
async def test_initialize_exchanges_invalid_exch():
    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    config["exchanges"]["binance"]["symbols"]["invalid_symbol"] = {}
    with pytest.raises(AttributeError, match = "Invalid symbol") as excinfo:
        exchanges = await initialize_exchanges(config)

# Completely Unrecognizable stream
async def test_initialize_exchanges_invalid_exch():
    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    config["exchanges"]["binance"]["symbols"]["BTC/USD:BTC"]["streams"]["invalid_stream"] = {}
    with pytest.raises(AttributeError, match = "Undefined stream") as excinfo:
        exchanges = await initialize_exchanges(config)

# Valid stream but not supported by the exchange
async def test_initialize_exchanges_invalid_exch():
    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    config["exchanges"]["binance"]["symbols"]["BTC/USD:BTC"]["streams"]["invalid_stream"] = {}
    with pytest.raises(AttributeError, match = "Undefined stream") as excinfo:
        exchanges = await initialize_exchanges(config)