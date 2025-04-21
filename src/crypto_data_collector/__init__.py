"""crypto_data_collector — async producer/consumer pipeline for websocket data."""

from .main import run_pipeline
from .consumer import BaseConsumer

__all__ = ["run_pipeline", "BaseConsumer"]