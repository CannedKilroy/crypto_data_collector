import asyncio
import logging
from pathlib import Path
from crypto_data_collector import run_pipeline, BaseConsumer
from crypto_data_collector.helpers import ConfigHandler, valid_config

# Load and validate config
cfg = ConfigHandler(config_path="config/config.yaml")
config = cfg.get_config()
valid_config(config)

# Define a consumer
class PrintConsumer(BaseConsumer):
    async def process(self, data):
        print(data)

# Run pipeline
if __name__ == "__main__":
    asyncio.run(
        run_pipeline(
            config=config,
            consumers=[PrintConsumer()],
            log_file_path=Path("logs/pipeline.log"),
            log_level=logging.INFO,
            console=True,
        )
    )