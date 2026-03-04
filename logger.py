import os
import sys

from loguru import logger


LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {message}"


def setup_logger():
    logger.remove()

    os.makedirs("logs", exist_ok=True)

    logger.add(
        sys.stdout,
        format=LOG_FORMAT,
        level=os.getenv("LOG_LEVEL", "INFO"),
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )

    logger.add(
        "logs/system.log",
        format=LOG_FORMAT,
        level="INFO",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )

    logger.add(
        "logs/error.log",
        format=LOG_FORMAT,
        level="ERROR",
        rotation="00:00",
        retention="45 days",
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=False,
    )


def log_status(channel, score, status, level="INFO"):
    logger.log(level, f"{channel} | score={score} | status={status}")
