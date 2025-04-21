import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class BaseConsumer(ABC):

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    async def process(self, data: Dict[str, Any]) -> None:
        pass

    async def run(self, queue: asyncio.Queue) -> None:
        while True:
            data = await queue.get()
            try:
                await self.process(data)
            except Exception:
                logger.exception("Error in consumer %s", self.name)
                raise