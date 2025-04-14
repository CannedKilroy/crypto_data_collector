import sys
import ccxt.pro
import asyncio
import datetime
import redis
import logging

from crypto_data_collector.helpers import ConfigHandler
from crypto_data_collector.consumer import Consumer
from crypto_data_collector.producer import data_producer, create_producers, initialize_exchanges


async def main():

    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()
    config_handler.valid_config(config)

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
    
    # Create Consumers
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
        sys.exit(" Bye Bye ")

if __name__ == "__main__":
    asyncio.run(main())