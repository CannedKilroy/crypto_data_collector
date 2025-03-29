"""
run from project root
poetry run python -m app.data_pipeline.main

Did: Remove logging
Idea: Type hinting by giving example

Double source of truth
:::::: TODO :::::::::::::::
0. Move main out of data_producer
1. Split code up now. Different files
2. Split up initializing exchanges
3. Logging
4. Setup the basic flask front end, to make sure it can interface with
5. Add other streams like open interest
6. Clean up imports and unused code
7. Optimize redis and figure out thread, conn pooling, cluster, async etc
   the way i have implemented redis storage.
- GO through redis datatypes throughly and
  learn more before hodepodging stuff together
- Ask first what traders ned
- Priority queue for first order data
- create standalone function that verifys a config
  then just loop through it. This allows me to add more
  exchanges and symbols on the fly later.
- Breakup initialize exchanges
- Second data consumer that writes to archival
  . Does not go through redis
- Break try catch running streams into own function
- Check how i injected meta data like exchange and symbol
on the other one, as some streams dont have that but i need it
- Implement exchange / ticker etc to check archival storage
"""

import sys
import ccxt.pro
import asyncio
import datetime
import redis
import logging

from typing import List, Callable, Union, Tuple, Optional

from .storage import Trades, Ticker, OHLCV, Orderbook
from .helpers import ConfigHandler, redis_connector
from .consumer import Consumer
from .producer import data_producer, create_producers

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
    exchange = None
    try:
        for exchange_name, configs in config['exchanges'].items():
            if exchange_name not in ccxt.pro.exchanges:
                raise AttributeError(f"Exchange '{exchange_name}' is invalid. Update config file")
            # If we already checked that exchange
            if exchange_name in valid_exchanges.keys():
                exchange = valid_exchanges[exchange_name]
            else:    
                exchange_class = getattr(ccxt.pro, exchange_name)
                exchange = exchange_class(configs['properties'])
                await exchange.close()
                valid_exchanges[exchange_name] = exchange
                #Later on pass into redis
                markets = await exchange.load_markets()
            
            for symbol, streams in configs['symbols'].items():
                if symbol not in exchange.symbols:
                    raise AttributeError("Invalid symbol. Update Config")
                    # log here
                for stream_dict in streams["streams"]:
                    stream_name=list(stream_dict.keys())[0]
                    if not exchange.has[stream_name]:
                        raise AttributeError("Invalid stream. Update Config")
                        # log here

        return valid_exchanges

    except AttributeError as e:
        if exchange is not None:
            await exchange.close()
        print(str(e))
        sys.exit("Cannot Continue with invalid options")
    
    except Exception as e:
        print(f"An error occurred while initializing {exchange_name}: {str(e)}")
        if exchange is not None:
            await exchange.close()
        sys.exit("Cannot Continue with invalid options")


async def main():
    print("DATA PIPELINE WRUNNING")
    conn = redis_connector()

    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    
    exchange_objects = await initialize_exchanges(config=config)

    data_queue = asyncio.Queue()
    producers = []
    consumers = []
    
    # Create Producers
    producers = create_producers(
        data_queue=data_queue,
        config=config,
        exchange_objects = exchange_objects,
        consumers=consumers,
        producers=producers
        )
    
    # Create Consumer
    consumer = Consumer()
    consumers.append(consumer.consumer_delegator(data_queue=data_queue))

    try:          
        async with asyncio.TaskGroup() as tg:
            for producer in producers:
                tg.create_task(producer)
            for consumer in consumers:
                tg.create_task(consumer)
    except TypeError as e:
        raise e
    finally:
        for exchange_name, exchange in exchange_objects.items():
            await exchange.close()
        conn.connection_pool.close()
        sys.exit(" Bye Bye ")        

if __name__ == "__main__":
    asyncio.run(main())