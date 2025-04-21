import asyncio
import logging
import ccxt.pro

from typing import Dict, Any, Optional, Union
from pathlib import Path

from crypto_data_collector.helpers import setup_logger, valid_config
from crypto_data_collector.consumer import BaseConsumer
from crypto_data_collector.producer import create_producers, initialize_exchanges

logger = logging.getLogger(__name__)

async def run_pipeline(
    config: Dict[str,Any],
    consumers: list[BaseConsumer],
    log_file_path: Optional[Union[str, Path]] = None,
    log_level: int = logging.INFO,
    console: bool = True,
    console_level: Optional[int] = None,
    ) -> None:
    """
    Run the data pipeline with the given config and consumers

    :param config: Python dict containing 'exchanges' and 'consumers' keys.
    :param consumers: List of BaseConsumer instances that will receive data.
    :param log_file_path: Optional path for the log file; if provided, logging is configured.
    "param log_level: The overall logging level (default INFO)
    :param console: Whether to enable console logging (default True).
    :param console_level: Specific logging level for console handler.
    """

    setup_logger(
        log_file_path,
        level=log_level,
        console=console,
        console_level=console_level,
    )
    logger.info("Pipeline startup with user-provided configuration.")

    
    data_queue = asyncio.Queue()
    
    
    exchange_objects: Dict[str, ccxt.pro.Exchange]
    exchange_objects = {}
    
    try:
        valid_config(config)

        exchange_objects = await initialize_exchanges(config=config)
        
        consumer_tasks = [c.run(data_queue) for c in consumers]
        producer_tasks = create_producers(
        data_queue=data_queue,
        config=config,
        exchange_objects = exchange_objects,
        producers=[]
        )

        async with asyncio.TaskGroup() as tg:
            for task in producer_tasks + consumer_tasks:
                tg.create_task(task)
    
    except asyncio.CancelledError:
        logger.info("Pipeline cancelled, shutting down...")
    
    except TypeError as e:
        logger.exception("Pipeline error (likely invalid config): %s", e)
        raise
    
    finally:
        for ex in exchange_objects.values():
            try:
                await ex.close()
            except Exception:
                logger.warning("Error closing exchange %s", ex)


if __name__ == "__main__":
    from crypto_data_collector.helpers import ConfigHandler

    class ExampleConsumer(BaseConsumer):
        async def process(self, data):
            print(data)
    
    consumers = [ExampleConsumer()]
    
    log_file_name = "logs.log"
    
    main_script_path = Path(__file__).resolve()
    project_root = main_script_path.parent.parent.parent
    log_file_path = project_root / "logs" / log_file_name

    config_handler = ConfigHandler()
    config_handler.generate_config()
    config = config_handler.get_config()

    asyncio.run(
        run_pipeline(
            config=config,
            consumers=consumers,
            log_file_path=log_file_path,
            log_level=logging.DEBUG,
            console=True,
            console_level=logging.INFO,
            )
        )