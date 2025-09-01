import logging
import ccxt.pro
import asyncio

from typing import List, Callable, Union, Tuple, Optional, Dict, Any, TYPE_CHECKING

from crypto_data_collector.helpers import State, Status

if TYPE_CHECKING:
    from crypto_data_collector.consumer import BaseConsumer

logger = logging.getLogger(__name__)

class ProducerPipeline:
    def __init__(self, data_queue:asyncio.Queue) -> None:
        # Producer pipeline owns the tasks
        self.producers : Dict[str, DataProducer] = {}
        self.data_queue: asyncio.Queue = data_queue
    
    async def stop_pipeline(self) -> None:
        for name, producer in self.producers.items():
            self.remove_producer(name)

    def get_data_queue(self) -> asyncio.Queue:
        return self.data_queue

    def add_producer(
        self,
        producer_name:str,
        producer:"DataProducer"
        ) -> None:

        if producer_name in self.producers:
            logger.warning("Producer %s already added, skipping", producer_name)
            return
        
        self.producers[producer_name] = producer
        task = asyncio.create_task(producer.start_loop(), name=producer.producer_name)
        producer.task = task
        logger.info("Task [%s] created", producer_name)
    
    async def remove_producer(self, producer_name: str) -> None:
        # Do not cancel task willy nilly, use this method
        producer = self.producers.get(producer_name, None)
        if not producer or not producer.task:
            logger.warning("Cannot remove producer [%s] that DNE or has no task", producer_name)
            return
        
        producer.task.cancel()
        producer.state.status = Status.CANCELLED

        try:
            await producer.task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("Error shutting down producer [%s]: [%s]", producer_name, e)
            producer.state.status = Status.ERRORED
        
        self.producers.pop(producer_name, None)
        exch = producer.exchange

        if not any(p.exchange is exch for p in self.producers.values()):
            logger.info("Closing exchange [%s]", exch.name)
            try:
                await exch.close()
            except Exception:
                logger.exception("Error closing exchange [%s]", exch.name)
        logger.info("Producer [%s] fully removed", producer_name)




class DataProducer:

    def __init__(
        self,
        exchange_name:str,
        exchange: ccxt.pro.Exchange,
        symbol:str,
        stream_name:str,
        stream_method: Callable[..., Any],
        stream_options: Dict[str, Any],
        data_queue: asyncio.Queue) -> None:

        # A Unique Producer name
        # These are names passed by you, not the ccxt names
        # Must be unique
        self.producer_name = f"{exchange_name}|{symbol}|{stream_name}"
        
        # Statuses are only for information, they are not used for loop control
        self.state = State()
        self.state.status = Status.STAGED

        self.exchange_name = exchange_name
        self.exchange = exchange
        self.symbol = symbol
        self.stream_name = stream_name

        self.stream_method = stream_method
        self.stream_options = stream_options

        self.data_queue = data_queue
        self.task: Optional[asyncio.Task] = None

        self.max_tries = 4

    async def start_loop(self) -> None:
        self.state.status = Status.RUNNING
        try:
            await self.run()
        except asyncio.CancelledError:
            raise

    async def run(self) -> None:
        while True:
            try:
                # Blocking await
                data = await self.stream_method(self.symbol, **self.stream_options)
            except ccxt.OperationFailed as e:
                # Transient Error handle with exponential backoff
                self.state.status = Status.BACKOFF
                self.state.tries += 1
                logger.error('OperationFailed for producer [%s] msg: %s', self.producer_name, repr(e))
                
                if self.state.tries >= self.max_tries:
                    logger.critical("Max retries exceeded in producer [%s]. Cancelling ... ", self.producer_name)
                    self.state.status = Status.ERRORED
                    raise asyncio.CancelledError()
                
                logger.info("Backing off for %.1f seconds (try #%d)", self.state.timeout, self.state.tries)
                await asyncio.sleep(self.state.timeout)

                self.state.timeout *= 2
                continue
            
            self.state.status = Status.RUNNING
            # Inject Metadata
            if isinstance(data, list):
                full_data = {"data": data, "producer": self.producer_name}
            elif isinstance(data, dict):
                full_data = data | {"producer": self.producer_name}
            else:
                logger.warning("Unsupported data type from stream: [%s]", self.producer_name)
                continue
            self.data_queue.put_nowait(full_data)

            self.state.timeout = 1.0
            self.state.tries = 0