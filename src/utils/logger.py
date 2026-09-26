"""
Centralized structured logging utility for Retail Data Engineering Pipeline.
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")


def get_logger(name: str = "retail_pipeline", log_level: str = "INFO") -> logging.Logger:
    """
    Configure and return a structured logger with console and rotating file output.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "pipeline.log")

    logger = logging.getLogger(name)
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Avoid duplicate handlers if already configured
    if logger.handlers:
        return logger

    # Log format
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)-8s] [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 2. Rotating File Handler (Max 10MB per file, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    return logger
