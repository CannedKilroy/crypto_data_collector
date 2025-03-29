from pathlib import Path
import yaml


# Generate, Load, Config for Testing
class ConfigHandler:
    """
    Loads config on initialization
    Can also reload config and call again
    """
    def __init__(self, config_path=None):
        self.config=None
        self.config_path=config_path
        self.load_config(self.config_path)

    def load_config(self, config_path=None):
        if config_path is None:
            current_script_path = Path(__file__).resolve()
            project_root = current_script_path.parent.parent
            print(project_root)
            config_path = project_root / 'config' / 'config.yaml'
        
        print("Current script path: ", current_script_path)    
        print("Config Path: ", config_path)
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

    def generate_config(self, load_test=False):
        """
        Generates a default config
        """
        current_script_path = Path(__file__).resolve()
        project_root = current_script_path.parent.parent
        config_path = project_root / 'config' / 'config.yaml'
        data = {
            "exchanges":{
                "binance":{
                    "properties":{
                        "enableRateLimit": True,
                        "async_support": True,
                        "newUpdates": True,
                        "verbose": False}
                        ,
                    "symbols":{
                        "BTC/USD:BTC":{
                            "streams":[
                                {"watchOHLCV":     {"options":{} }},
                                {"watchTicker":    {"options":{} }},
                                {"watchTrades":    {"options":{} }},
                                {"watchOrderBook": {"options":{} }}
                            ]
                        },
                        "BTC/USDT:USDT":{
                            "streams":[
                                {"watchOHLCV":     {"options":{} }},
                                {"watchTicker":    {"options":{} }},
                                {"watchTrades":    {"options":{} }},
                                {"watchOrderBook": {"options":{} }}
                            ]
                        }
                    }
                },
                "bitmex":{
                    "properties":{
                        "enableRateLimit": True,
                        "async_support": True,
                        "newUpdates": True,
                        "verbose": False,
                        "timeout":1000}
                        ,
                    "symbols":{
                        "BTC/USD:BTC":{
                            "streams":[
                                {"watchOHLCV":     {"options":{} }},
                                {"watchTicker":    {"options":{} }},
                                {"watchTrades":    {"options":{} }},
                                {"watchOrderBook": {"options":{} }}
                            ]
                        },
                        "BTC/USDT:USDT":{
                            "streams":[
                                {"watchOHLCV":     {"options":{} }},
                                {"watchTicker":    {"options":{} }},
                                {"watchTrades":    {"options":{} }},
                                {"watchOrderBook": {"options":{} }}
                            ]
                        }
                    }
                }
            }
        }
        if load_test:
            return data
        else:
            with open(config_path, 'w') as file:
                yaml.dump(data=data, stream=file, default_flow_style=False)

def load_test():
    # Adds many symbols and exchanges to see how my script handles it
    pass