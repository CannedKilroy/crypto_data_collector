import logging
import datetime
import ccxt.pro
import sys
import asyncio

from typing import List, Callable, Union, Tuple, Optional, Dict, Any

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
    data_queue: asyncio.Queue,
    symbol_name: str,
    stream_method: Callable,
    stream_name:str,
    stream_options: Dict[str, any],
    stream_info:Dict[str, any]) -> None:
    """
    Continuously fetch data from a CCXT Pro websocket stream and enqueue it.

    :param data_queue: The shared asyncio.Queue to publish messages to.
    :param symbol_name: The market symbol (e.g. "BTC/USD").
    :param stream_method: The CCXT Pro async method (e.g. exchange.watchTrades).
    :param stream_name: Name of the stream (e.g. "watchTrades").
    :param stream_options: Keyword args to pass into `stream_method`.
    :param stream_info: Stream info to merge into each message.
    """
    while True:
        try:
            data = await stream_method(symbol_name, **stream_options)
            if isinstance(data, list):
                full_data = data + list(stream_info.values())
            elif isinstance(data, dict):
                full_data = data | stream_info
            await data_queue.put(full_data)
        except asyncio.CancelledError:
            logger.info("Producer for %s.%s cancelled", symbol_name, stream_name)
            break
        except Exception as e:
            logger.exception(
                "Error in data_producer [%s:%s]: %s", symbol_name, stream_name, e
            )
            await asyncio.sleep(1.0)


def create_producers(
    data_queue: asyncio.Queue,
    config: Dict[str, Any],
    exchange_objects: Dict[str, ccxt.pro.Exchange],
    ) -> List:
    """
    Build and schedule a producer Task for every (exchange, symbol, stream).

    :param data_queue: Shared queue to feed into.
    :param config: The validated config dict.
    :param exchange_objects: Mapping exchange_name → ccxt.pro.Exchange instance.
    :return: A list of Coroutines with their info
    """

    producers = []
    for exchange_name, exchange in exchange_objects.items():
        for symbol_name, symbol_streams in config['exchanges'][exchange_name]['symbols'].items():
            for stream_name, stream_options in symbol_streams['streams'].items():
                
                producer_info = {
                    "exchange_name": exchange_name,
                    "symbol_name": symbol_name,
                    "stream_name": stream_name
                    }
                
                stream_options = stream_options.get('options', {})
                stream_method = getattr(exchange, stream_name)

                producer = data_producer(
                    data_queue=data_queue,
                    symbol_name=symbol_name,
                    stream_method=stream_method,
                    stream_name=stream_name,
                    stream_options=stream_options,
                    stream_info=producer_info
                    )
                
                producers.append((producer, producer_info))
    return producers