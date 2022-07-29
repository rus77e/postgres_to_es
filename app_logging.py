import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path

LOG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "logs")

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "app": {
            "format": "%(asctime)s - %(message)s",
            "datefmt": r"%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "app"
        }
    },
    "root": {"level": "DEBUG"},
    "loggers": {
        "app": {"handlers": ["console"]}
    },
})


def get_logger() -> logging.Logger:
    """
    Return configured logger instance.

    Returns:
        logging.Logger: Logger instance
    """
    logger = logging.getLogger("app")
    try:
        Path(LOG_DIR).mkdir(exist_ok=True, parents=True)
        formatter = logging.Formatter(fmt="%(asctime)s - %(message)s", datefmt=r"%Y-%m-%d %H:%M:%S")
        handler = TimedRotatingFileHandler(
            interval=6,
            backupCount=5,
            when="D",
            filename=os.path.join(LOG_DIR, "postgres_to_es.log")
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except PermissionError:
        logger.error(f"Wrong permission for {LOG_DIR}. No logging to file available.")

    return logger
