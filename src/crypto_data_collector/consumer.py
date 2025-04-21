import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

logger = logging.getLogger(__name__)

class BaseConsumer(ABC):

    @abstractmethod
    async def process(self, data: Dict[str, Any]) -> None:
        pass

    async def run(self, queue: asyncio.Queue) -> None:
        while True:
            data = await queue.get()
            try:
                await self.process(data)
            except Exception:
                logger.exception("Error in user consumer %s", self.__class__.__name__)
                raise