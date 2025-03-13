import os
import sys
from pathlib import Path

from loguru import logger

def setup_logger():
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    log_file = logs_dir / "dom_collector.log"
    
    config = {
        "handlers": [
            {
                "sink": sys.stderr,
                "format": "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
                "level": "INFO",
            },
            {
                "sink": str(log_file),
                "format": "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
                "level": "DEBUG",
                "rotation": "10 MB",
                "retention": "1 week",
                "compression": "zip",
            },
        ],
    }
    
    logger.configure(**config)
    return logger

logger = setup_logger() 