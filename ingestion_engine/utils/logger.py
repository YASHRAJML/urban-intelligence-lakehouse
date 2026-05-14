"""Centralized logging configuration for the ingestion engine."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any


def get_logger(name: str, level: str | None = None) -> logging.Logger:
    """
    Get a configured logger for the given module name.

    Usage:
        logger = get_logger(__name__)
    """
    log_level = level or os.getenv("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Already configured

    logger.setLevel(numeric_level)

    # Console handler with rich formatting
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
