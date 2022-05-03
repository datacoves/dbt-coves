"""
Global dbt-coves exception and warning classes.
"""


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
