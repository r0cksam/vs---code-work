"""Logging helpers.

The app should not silently swallow data errors. These helpers keep the UI alive
while preserving useful debugging information in the terminal/log file.
"""
from __future__ import annotations
import logging
import os
from pathlib import Path

LOG_DIR = Path(os.getenv("PDASH_LOG_DIR", ".pdash_logs"))
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pdash.log"

logger = logging.getLogger("pDash")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(stream)
    logger.addHandler(file_handler)


def log_warning(context: str, exc: Exception | None = None) -> None:
    if exc is None:
        logger.warning(context)
    else:
        logger.warning("%s: %s", context, exc, exc_info=False)


def log_info(message: str) -> None:
    logger.info(message)
