import logging
from pathlib import Path

from rich.logging import RichHandler


class Logger:
    def __init__(self):
        logger = logging.getLogger("dbt-coves logger")
        logger.setLevel(logging.INFO)

        c_handler = RichHandler(
            rich_tracebacks=True,
            show_level=False,
            markup=True,
            enable_link_path=False,
            show_path=False,
        )

        c_handler.setLevel(logging.INFO)
        logger.addHandler(c_handler)

        self.logger = logger
        self.format = format

    def set_debug(self):
        """Set loggers handlers to debug level."""
        self.logger.setLevel(logging.DEBUG)
        for handler in self.logger.handlers:
            handler.setLevel(logging.DEBUG)


log_manager = Logger()
LOGGER = log_manager.logger
