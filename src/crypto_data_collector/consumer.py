import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ConsumerPipeline():
    def __init__(self, data_queue: asyncio.Queue, name: Optional[str] = None):
        self.data_queue: asyncio.Queue = data_queue
        self.name: str = name or self.__class__.__name__
        self.consumers = {}

    async def consumer_delegator(self):
        logger.info("Consumer Delegator started")
        try:
            while True:
                data = await self.data_queue.get()
                for consumer in self.consumers.values():
                    consumer.get_data_queue().put_nowait(data)
                self.data_queue.task_done()
        except asyncio.CancelledError:
            logger.warning("Delegator called to be cancelled")
            logger.info("Emptying queue before cancelling...")
            while True:
                try:
                    data = self.data_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    for consumer in self.consumers.values():
                        consumer.get_data_queue().put_nowait(data)
                    self.data_queue.task_done()
            logger.info("Delegator Queue emptied. Exiting")
            raise
        except Exception:
            logger.exception("Unhandled error in consumer delegator")
            raise

    def add_consumer(
        self,
        name: str,
        consumer: "BaseConsumer"
        ) -> None:
        if name in self.consumers:
            logger.warning("Consumer [%s] already added, skipping", name)
            return
        self.consumers[name] = consumer
        consumer.set_status("staged")
        task = asyncio.create_task(consumer.start_loop(), name=consumer.name)
        task.add_done_callback(consumer.task_done_callback)
        consumer.task = task
        logger.info("Consumer task [%s] created", name)
    
    async def remove_consumer(self, consumer_name:str) -> None:
        consumer = self.consumers.get(consumer_name, None)
        if not consumer or not consumer.task:
            logger.info("Cannot remove consumer [%s] that DNE or has no task", consumer_name)
            return
        consumer.task.cancel()
        try:
            await consumer.task
        except:
            pass
        self.consumers.pop(consumer_name)
        logger.info("Consumer [%s] fully removed", consumer_name)

    def get_data_queue(self) -> asyncio.Queue:
        return self.data_queue




class BaseConsumer(ABC):
    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.task = None
        self.status = None
        self.data_queue: asyncio.Queue = asyncio.Queue()
    
    def get_data_queue(self) -> asyncio.Queue:
        return self.data_queue

    def task_done_callback(self, task:asyncio.Task) -> None:
        ######## Do something with the callback########
        ######## Put your code here ################
        pass

    def set_status(self, status: str) -> None:
        self.status = status
    
    async def start_loop(self) -> None:
        self.status = "running"
        try:
            await self.run()
        except asyncio.CancelledError:
            raise

    @abstractmethod
    async def run(self, data: Dict[str, Any]) -> None:
        pass

    def get_name(self) -> str:
        return self.name
