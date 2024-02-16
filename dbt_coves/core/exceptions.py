"""
Global dbt-coves exception and warning classes.
"""

import pathlib


class DbtCovesException(Exception):
    """General Exception"""

    pass


class YAMLFileEmptyError(DbtCovesException):
    """Thrown when a yamlfile existed but had nothing in it."""


class MissingDbtProject(DbtCovesException):
    """Thrown when one or more in-scope dbt projects could not be found."""


class MissingCommand(DbtCovesException):
    """Thrown when no command was specified and we want to show the help."""

    def __init__(self, arg_parser):
        self.arg_parser = arg_parser
        super().__init__()

    def print_help(self):
        self.arg_parser.print_help()


class MissingArgumentException(DbtCovesException):
    """Thrown when an argument is missing."""

    # Receives a list of arguments and the config of the task
    def __init__(self, args, config=None):
        self.config = config
        message = f"Task requires the following arguments: [red]{' '.join(args)}[/red]"
        if config and config._config_path == pathlib.Path(str()):
            # If config_path wasn't overwritten it means config.yml doesn't exist
            message += (
                "\nNo [yellow].dbt_coves/config.yml[/yellow] found, "
                "visit https://github.com/datacoves/dbt-coves?#settings for details"
            )
        super().__init__(message)
