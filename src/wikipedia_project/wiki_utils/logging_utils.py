"""
Centralized logging configuration for the application
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

class ApplicationLogger:
    def __init__(
        self,
        log_dir: Path,
        logger_name: str = __name__,
        debug: bool = False,
        verbose: bool = True,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5
    ):
        self.log_dir = Path(log_dir).expanduser()
        self.logger_name = logger_name
        self.debug = debug
        self.verbose = verbose
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.logger = self._configure_logger()

    def _configure_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.logger_name)
        logger.handlers.clear()
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=self.log_dir / f"{self.logger_name}.log",
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_level = logging.DEBUG if self.verbose else logging.INFO
        console_handler.setLevel(console_level)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logging.captureWarnings(True)

        return logger

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        if name:
            return self.logger.getChild(name)
        return self.logger