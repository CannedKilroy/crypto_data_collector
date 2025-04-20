import logging
import datetime
import ccxt.pro
import sys
from typing import Dict, Any

from typing import List, Callable, Union, Tuple, Optional

logger = logging.getLogger(__name__)

async def initialize_exchanges(
    config: Dict[str, Any]
    ) -> Dict[str, ccxt.pro.Exchange]:
    '''
    Initializes and returns a dictionary of CCXT Pro
    exchange objects for each exchange name provided.
    Only exchanges and symbols supported by CCXT Pro are initialized.
    Invalid configs are ignored by ccxt so no need to check

    :param config: A dict to initialize
    :return:
        A dict mapping valid exchange names to ccxt.pro.Exchange instances.
    '''
    valid_exchanges: Dict[str, ccxt.pro.Exchange] = {}
    for exchange_name, configs in config.get('exchanges',{}).items():
        exchange = None
        try:
            if exchange_name in valid_exchanges.keys():
                exchange = valid_exchanges[exchange_name]
            elif exchange_name not in ccxt.pro.exchanges:
                raise AttributeError(f"Invalid exchange: {exchange_name}")
            else:
                exchange_class = getattr(ccxt.pro, exchange_name)
                exchange = exchange_class(configs['properties'])
                valid_exchanges[exchange_name] = exchange
                markets = await exchange.load_markets()
            for symbol, streams in configs['symbols'].items():
                if symbol not in exchange.symbols:
                    raise AttributeError(f"Invalid symbol: {symbol}")
                for stream_name, stream_dict in streams["streams"].items():
                    support = exchange.has.get(stream_name, None)                       
                    if stream_name not in exchange.has:
                        raise AttributeError(f"Undefined stream: {stream_name} Check spelling")
                    if support is None:
                        raise AttributeError(f"Method '{stream_name}' is not yet implemented in CCXT")
                    if support is False:
                        raise AttributeError(f"Unsupported stream: '{stream_name}' on exchange '{exchange_name}'")
        except Exception as e:
            logger.exception("%s",e)
            for ex in valid_exchanges.values():
                try:
                    await ex.close()
                except Exception:
                    pass
            raise
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