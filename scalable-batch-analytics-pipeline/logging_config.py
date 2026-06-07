"""Structured logging for pipeline stages."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import LOGS_DIR, PIPELINE_RUNS_LOG


def setup_logger(name: str, log_file: str | None = None) -> logging.Logger:
    """Configure a logger with console and optional file handlers."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        file_handler = logging.FileHandler(LOGS_DIR / log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_pipeline_event(stage: str, status: str, metrics: dict[str, Any] | None = None) -> None:
    """Append a JSON line to the pipeline run audit log."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "status": status,
        "metrics": metrics or {},
    }
    with PIPELINE_RUNS_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")
