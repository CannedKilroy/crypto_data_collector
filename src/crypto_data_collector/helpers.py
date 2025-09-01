# Optional Helper Functions

import time
import logging
import yaml

from pathlib import Path
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Union, Optional, Dict, Any, List
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)

class Status(Enum):
    STAGED = auto()
    RUNNING = auto()
    BACKOFF = auto()
    CANCELLED = auto()
    ERRORED = auto()

@dataclass(frozen=False)
class State():
    status: Status | None = None
    tries: int = 0
    timeout: float = 0.0
    last_error: str | None = None
    since: float = field(default_factory=lambda: time.time())


def get_nested(data: dict, path: list, default=None):
    """
    Gets a nested key in a dict following a path

    Args:
        data (dict): Dict to traverse
        path (list): Keys to get in dict, in order
        default (None): Default value to return if cannot traverse
    Returns:
        Value
    """
    current = data
    for key in path:
        if isinstance(current, dict) and key in current.keys():
            current = current[key]
        else:
            return default
    return current

def producer_name_parser(producer_name:str) -> List[str]:
    """
    Parse Producer Name
    Format Should be:
            "exchange_name|symbol|stream_name"
    """
    return producer_name.split("|")

def setup_logger(
    log_file_path: Optional[Union[str, Path]] = None,
    level: int = logging.INFO,
    console: bool = True,
    console_level: Optional[int] = None
    ) -> logging.Logger:
    """
    Optional Helper Logger Function
    Configures the root logger to output to a rotating file (if path provided)
    and optionally to the console.

    :param log_file_path: Path or filename for the log file. If None, skip file logging.
    :param level: Logging level for file and console (if console_level not set).
    :param console: Whether to enable console (stdout) logging.
    :param console_level: Logging level for console handler (defaults to `level`).
    :return: The configured root logger instance.
    """
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s (in %(pathname)s:%(lineno)d)"
    )
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)
    if log_file_path:
        log_file_path = Path(log_file_path)
        if not log_file_path.parent.exists():
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=str(log_file_path), maxBytes=10_240, backupCount=2
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level or level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    return root_logger


class ConfigHandler:
    """
    Optional ConfigHandler Class

    Provides an example of a valid config structure 
    using the YAML config in config/
    This structure is not enforced, it is simply an example.
    """
    def __init__(
        self,
        config_override:Optional[Dict[str, Any]] = None,
        project_root: Optional[Path] = None,
        ) -> None:
        """
        Initialize the config handler.

        Args:
            config_override (Optional[Dict[str, Any]]): Custom config provided by the user.
            project_root (Optional[Path]): Root directory of the project for default config loading.
        """
        
        if config_override is not None:
            self.config = config_override
        elif project_root is not None:
            self.config = self._get_default_config(project_root)
        else:
            raise ValueError("Either config_override or project_root must be provided.")

    def _get_default_config(self, project_root) -> Dict[str,Any]:
        """
        Load default YAML config from project root dir
        """
        config_path = project_root / 'config' / 'producers.yaml'
        
        if not config_path.exists():
            raise FileNotFoundError(f"Default config file not found: {config_path}")

        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def get_config(self) -> Dict[str, Any]:
        return self.config
