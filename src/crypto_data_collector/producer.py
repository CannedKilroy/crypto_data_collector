import logging
import ccxt.pro
import asyncio

from typing import List, Callable, Union, Tuple, Optional, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from crypto_data_collector.consumer import BaseConsumer

logger = logging.getLogger(__name__)

class ProducerPipeline:
    def __init__(self) -> None:
        # Producer pipeline owns the tasks
        self.producers : Dict[str, DataProducer] = {}
        self.data_queue: asyncio.Queue = asyncio.Queue()
    
    async def stop_pipeline(self) -> None:
        for name, producer in self.producers.items():
            self.remove_producer(name)

    def get_producer_state(self, producer_name:str):
        return self.producers[producer_name].get_status()

    def get_producers(self) -> Dict[str, "DataProducer"]:
        return self.producers

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
        producer.set_status("staged")
        task = asyncio.create_task(producer.start_loop(), name=producer.get_name())
        producer.task = task
        logger.info("Task [%s] created", producer_name)
    
    async def remove_producer(self, producer_name: str) -> None:
        # Do not cancel task willy nilly, use this method
        producer = self.producers.get(producer_name, None)
        if not producer or not producer.task:
            logger.warning("Cannot remove producer [%s] that DNE or has no task", producer_name)
            return
        
        producer.task.cancel()
        producer.set_status("cancelled")

        try:
            await producer.task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("Error shutting down producer [%s]: [%s]", producer_name, e)
        
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

    # VALID_STATES = {
    #     "staged",    # Producer is staged but not running
    #     "running",   # Producer running
    #     "backoff",   # Producer backing off due to transient error
    #     "stopped",   # Prodcuer stopped 
    #     "cancelled", # Producer cancelled
    #     "error",     # Producer Errored out
    # }

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

        self.exchange_name = exchange_name
        self.exchange = exchange
        self.symbol = symbol
        self.stream_name = stream_name

        self.stream_method = stream_method
        self.stream_options = stream_options

        self.data_queue = data_queue

        self.status: str = "stopped"
        self.task: Optional[asyncio.Task] = None

        # Exponential backoff vars
        self.timeout = 0
        self.tries = 0
        self.max_tries = 4

    async def start_loop(self) -> None:
        self.status = "running"
        try:
            await self.run()
        except asyncio.CancelledError:
            raise

    def get_name(self) -> str:
        return self.producer_name
    
    def set_status(self, status:str) -> None:
        # Cancelled status only set on the cancel callback
        # Statuses are only for information, they are not used for loop control
        self.status = status

    async def run(self) -> None:
        self.timeout = 1.0
        self.tries = 0
        while True:
            try:
                # Blocking await
                data = await self.stream_method(self.symbol, **self.stream_options)
            except ccxt.OperationFailed as e:
                # Transient Error
                self.status = "backoff"
                self.tries += 1
                logger.error('OperationFailed for [%s] msg: %s', self.producer_name, repr(e))
                
                if self.tries >= self.max_tries:
                    logger.critical("Max retries exceeded in producer [%s]. Cancelling ... ", self.producer_name)
                    raise asyncio.CancelledError()
                
                logger.info("Backing off for %.1f seconds (try #%d)", self.timeout, self.tries)
                await asyncio.sleep(self.timeout)

                self.timeout *= 2
                continue
            
            self.status = "running"
            # Inject Metadata
            if isinstance(data, list):
                full_data = {"data": data, "producer": self.producer_name}
            elif isinstance(data, dict):
                full_data = data | {"producer": self.producer_name}
            else:
                logger.warning("Unsupported data type from stream: [%s]", self.producer_name)
                continue
            self.data_queue.put_nowait(full_data)

            self.timeout = 1.0
            self.tries = 0