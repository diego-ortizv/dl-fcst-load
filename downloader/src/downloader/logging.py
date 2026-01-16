"""Logging formatter."""
import datetime
import logging
from zoneinfo import ZoneInfo

tz_local = ZoneInfo("America/Lima")

class LoggerFormatter(logging.Formatter):
    """Formatter for Peruvian timezone."""

    def formatTime(  # noqa: N802
        self, record: logging.LogRecord, datefmt: str|None=None,
    ) -> str:
        """Localize America/Lima logging."""
        dt = datetime.datetime.fromtimestamp(record.created, tz=tz_local)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

def setup_logger(level:int=logging.INFO) -> None:
    """Configure logger with Peru timezone-aware formatter."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        LoggerFormatter(
            fmt="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S%z",
        ),
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
