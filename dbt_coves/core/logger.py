import logging
from pathlib import Path

from rich.logging import RichHandler


class LogManager:

    def __init__(
        self,
        log_file_path: Path = Path(Path.cwd(), "dbt_coves_logs"),
        log_to_console: bool = True,
    ):
        Path(log_file_path).mkdir(parents=True, exist_ok=True)

        log_filename = Path(log_file_path, "dbt_coves_log.log")
        logger = logging.getLogger("dbt-coves logger")
        # set the logger to the lowest level (then each handler will have it's level --this ensures
        # that all logging always ends up in the file logger.)
        logger.setLevel(logging.DEBUG)
        # Create handlers
        f_handler = logging.FileHandler(log_filename)
        f_handler.setLevel(logging.DEBUG)

        # Create formatters and add it to handlers
        f_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
        )

        f_handler.setFormatter(f_format)
        # Add handlers to the logger
        logger.addHandler(f_handler)

        # if we want to print the log to console we're going to add a streamhandler
        if log_to_console:
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
        self.f_format = f_format

    def set_debug(self):
        """Set all loggers handlers to debug level."""
        self.logger.setLevel(logging.DEBUG)
        for handler in self.logger.handlers:
            handler.setLevel(logging.DEBUG)


log_manager = LogManager()

GLOBAL_LOGGER = log_manager.logger
