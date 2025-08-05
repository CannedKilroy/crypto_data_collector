"""
Example usage

Usage:
    poetry run python -m crypto_data_collector
"""
import asyncio
import logging

from pathlib import Path

from crypto_data_collector.consumer import ConsumerPipeline, BaseConsumer
from crypto_data_collector.producer import ProducerPipeline, DataProducer
from crypto_data_collector.helpers import ConfigHandler, setup_logger
from crypto_data_collector.registry import Registry

logger = logging.getLogger(__name__)

# Example Consumers
class ExampleConsumer(BaseConsumer):
	async def run(self):
		try:
			while True:
				data = await self.data_queue.get()
				try:
					######## Do something with the data ########
					######## Put your code here ################
					print(f"{self.name} ran")
				finally:
					self.data_queue.task_done()
				
		except asyncio.CancelledError:
			logger.info("Consumer [%s] marked as cancelled. Emptying its data queue...", self.name)
			while True:
				try:
					data = self.data_queue.get_nowait()
					######## Do something with the data ########
					######## Put your code here ################
				except asyncio.QueueEmpty:
					logger.info("Consumer [%s] data queue emptied", self.name)
					break
				else:
					try:
						######## Do something with the data ########
						######## Put your code here ################
						pass
					finally:
						self.data_queue.task_done()
			raise

class ExampleConsumer2(BaseConsumer):
	async def run(self):
		try:
			while True:
				data = await self.data_queue.get()
				try:
					######## Do something with the data ########
					######## Put your code here ################
					print(f"{self.name} ran")
				finally:
					self.data_queue.task_done()
				
		except asyncio.CancelledError:
			logger.info("Consumer [%s] marked as cancelled. Emptying its data queue...", self.name)
			while True:
				try:
					data = self.data_queue.get_nowait()
					######## Do something with the data ########
					######## Put your code here ################
				except asyncio.QueueEmpty:
					logger.info("Consumer [%s] data queue emptied", self.name)
					break
				else:
					try:
						######## Do something with the data ########
						######## Put your code here ################
						pass
					finally:
						self.data_queue.task_done()
			raise
	

async def main():
	# Optional Logger Setup
	log_file_name = "logs.log"
	log_level = 20
	console_level = 20

	main_script_path = Path(__file__).resolve()
	project_root = main_script_path.parent.parent.parent
	log_file_path = project_root / "logs" / log_file_name
	setup_logger(
		log_file_path,
		level=log_level,
		console=True,
		console_level=console_level,
	)
	logger.info("Crypto Pipeline Project Startup")

	# Example config, edit as needed or provide override
	# This Structure isn't enforced
	config_handler = ConfigHandler(project_root=project_root)
	config = config_handler.get_config()

	# Instantiate Pipelines and Registry
	registry = Registry()
	producer_pipeline = ProducerPipeline()
	data_queue = producer_pipeline.get_data_queue()
	consumer_pipeline = ConsumerPipeline(data_queue)

	# Register all exchanges, symbols, and streams from the config
	for exchange_name, exch_data in config["exchanges"].items():
		await registry.register_exchange(exchange_name, exch_data["properties"])
		for symbol, symbol_data in exch_data["symbols"].items():
			await registry.register_symbol(exchange_name, symbol)
			for stream_name, stream_info in symbol_data["streams"].items():
				stream_args = stream_info.get('options', {})
				await registry.register_stream(exchange_name, symbol, stream_name, stream_args)
				
				exch_obj = registry.get_exchange_object(exchange_name)
				stream_method = registry.get_stream_method(exchange_name, symbol, stream_name)

				producer = DataProducer(
					exchange_name=exchange_name,
					exchange=exch_obj,
					symbol=symbol,
					stream_name=stream_name,
					stream_method=stream_method,
					stream_options=stream_args,
					data_queue=producer_pipeline.get_data_queue()
				)

				producer_pipeline.add_producer(
					producer_name = producer.get_name(),
					producer = producer
				)


	exampleconsumer = ExampleConsumer()
	exampleconsumer2 = ExampleConsumer2()

	# register each example consumer instance and create tasks
	consumer_pipeline.add_consumer(name="ExampleConsumer", consumer=exampleconsumer)
	consumer_pipeline.add_consumer(name="ExampleConsumer2", consumer=exampleconsumer2)

	asyncio.create_task(
		consumer_pipeline.consumer_delegator(),
		name="consumer_delegator"
		)

	await consumer_pipeline.remove_consumer("ExampleConsumer")

	await producer_pipeline.remove_producer("binance|BTC/USD:BTC|watchOHLCV")
	await producer_pipeline.remove_producer("binance|BTC/USD:BTC|watchTicker")
	await producer_pipeline.remove_producer("binance|BTC/USD:BTC|watchTrades")
	await producer_pipeline.remove_producer("binance|BTC/USD:BTC|watchOrderBook")

	await producer_pipeline.remove_producer("binance|BTC/USDT:USDT|watchOHLCV")
	await producer_pipeline.remove_producer("binance|BTC/USDT:USDT|watchTicker")
	await producer_pipeline.remove_producer("binance|BTC/USDT:USDT|watchTrades")
	await producer_pipeline.remove_producer("binance|BTC/USDT:USDT|watchOrderBook")
	# Binance should close now

	await producer_pipeline.remove_producer("bitmex|BTC/USD:BTC|watchOHLCV")
	await producer_pipeline.remove_producer("bitmex|BTC/USD:BTC|watchTicker")
	await producer_pipeline.remove_producer("bitmex|BTC/USD:BTC|watchTrades")
	await producer_pipeline.remove_producer("bitmex|BTC/USD:BTC|watchOrderBook")

	await producer_pipeline.remove_producer("bitmex|BTC/USDT:USDT|watchOHLCV")
	await producer_pipeline.remove_producer("bitmex|BTC/USDT:USDT|watchTicker")
	await producer_pipeline.remove_producer("bitmex|BTC/USDT:USDT|watchTrades")
	await producer_pipeline.remove_producer("bitmex|BTC/USDT:USDT|watchOrderBook")
	# Bitmex should close now

	await consumer_pipeline.remove_consumer("ExampleConsumer2")

	await asyncio.Event().wait()	

asyncio.run(main(), debug=False)