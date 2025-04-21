"""crypto_data_collector — async producer/consumer pipeline for websocket data."""
import logging

from .main import run_pipeline
from .consumer import BaseConsumer

__all__ = ["run_pipeline", "BaseConsumer"]

logging.getLogger(__name__).addHandler(logging.NullHandler())