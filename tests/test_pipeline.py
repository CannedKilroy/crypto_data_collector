import pytest

from src.crypto_data_collector.helpers import ConfigHandler
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
