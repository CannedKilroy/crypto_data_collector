import datetime
import ccxt.pro

from typing import List, Callable, Union, Tuple, Optional

async def initialize_exchanges(
    config: dict) -> Tuple[dict[str, ccxt.pro.Exchange], dict[str, ccxt.pro.Exchange]]:
    '''
    Initializes and returns a dictionary of CCXT Pro
    exchange objects for each exchange name provided.
    Only exchanges supported by CCXT Pro are initialized.
    Unsupported exchanges throw an exception and are skipped.
    Each exchange object is configured with rate limit enabled,
    asynchronous support, new updates, and verbosity.

    Does not make sense to initialize multiple times

    :param exchange_names: A list of exchange names (str) to be initialized.
    :return: A dictionary where keys are exchange names
             (str) and values are corresponding ccxt.pro.Exchange objects.
             Only successfully initialized exchanges are included.
    '''
    valid_exchanges = {}
    
    for exchange_name, configs in config['exchanges'].items():
        exchange = None
        try:
            if exchange_name in valid_exchanges.keys():
                exchange = valid_exchanges[exchange_name]
            elif exchange_name not in ccxt.pro.exchanges:
                raise AttributeError(f"Invalid exchange: {exchange_name}")
                # log here
            else:
                exchange_class = getattr(ccxt.pro, exchange_name)
                exchange = exchange_class(configs['properties'])
                # await exchange.close() # WHY DID I DO THIS
                valid_exchanges[exchange_name] = exchange
                markets = await exchange.load_markets()
            
            for symbol, streams in configs['symbols'].items():
                if symbol not in exchange.symbols:
                    raise AttributeError("Invalid symbol. Update Config")
                    # log here
                for stream_name, stream_dict in streams["streams"].items():
                    if not exchange.has[stream_name]:
                        raise AttributeError("Invalid stream. Update Config")
                        # log here

        except AttributeError as e:
            if exchange is not None:
                await exchange.close()
            print(str(e))
            raise e
            sys.exit("Cannot Continue with invalid options")
        
        except Exception as e:
            print(f"An error occurred while initializing {exchange_name}: {str(e)}")
            if exchange is not None:
                await exchange.close()
            sys.exit("Cannot Continue with invalid options")
        return valid_exchanges

async def data_producer(
    data_queue,
    symbol_name: str,
    stream_method,
    stream_name:str,
    stream_options: dict[str, any],
    meta_data:dict):
    while True:
        data = await stream_method(symbol_name, **stream_options)
        if isinstance(data, list):
            full_data = data + list(meta_data.values())
        elif isinstance(data, dict):
            full_data = data | meta_data
        await data_queue.put(full_data)

def create_producers(data_queue, config, exchange_objects, consumers, producers):
    """
    Meta Data injects data not in ws response
    """
    for exchange_name, exchange in exchange_objects.items():
        for symbol_name, symbol_streams in config['exchanges'][exchange_name]['symbols'].items():
            for stream_name, stream_options in symbol_streams['streams'].items():
                meta_data = {
                    "exchange_name": exchange_name,
                    "symbol_name": symbol_name,
                    "stream_name": stream_name
                    }
                
                stream_options = stream_options['options']
                stream_method = getattr(exchange, stream_name)
                producers.append(data_producer(
                    data_queue=data_queue,
                    symbol_name=symbol_name,
                    stream_method=stream_method,
                    stream_name=stream_name,
                    stream_options=stream_options,
                    meta_data=meta_data
                    ))
    return producers