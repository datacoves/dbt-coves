import logging
from pathlib import Path

from rich.logging import RichHandler


class Logger:
    def __init__(
        self,
        log_file_path: Path = Path(Path.cwd(), "dbt_coves_logs"),
        log_to_console: bool = True,
    ):

        Path(log_file_path).mkdir(parents=True, exist_ok=True)

        filename = Path(log_file_path, "dbt_coves_log.log")
        logger = logging.getLogger("dbt-coves logger")

        logger.setLevel(logging.DEBUG)

        # Handlers
        handler = logging.FileHandler(filename)
        handler.setLevel(logging.DEBUG)

        # Formatters
        format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
        )

        handler.setFormatter(format)

        # Add handlers
        logger.addHandler(handler)

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
        self.format = format

    def set_debug(self):
        """Set loggers handlers to debug level."""
        self.logger.setLevel(logging.DEBUG)
        for handler in self.logger.handlers:
            handler.setLevel(logging.DEBUG)


LOGGER = Logger().logger
