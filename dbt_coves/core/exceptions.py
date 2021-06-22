"""
Global dbt-coves exception and warning classes.
"""


class DbtCovesException(Exception):
    """General Exception"""

    pass


class YAMLFileEmptyError(DbtCovesException):
    """Thrown when a yamlfile existed but had nothing in it."""


class DbtProfileFileMissing(DbtCovesException):
    """Thrown when the `profiles.yml` cannot be found in its expected or provided location."""


class ProfileParsingError(DbtCovesException):
    """Thrown when no target entry could be found."""


class ConfigNotFoundError(DbtCovesException):
    """Thrown when a config could not be extracted from the .dbt_coves file."""


class NoConfigProvided(DbtCovesException):
    """Thrown when neither a default config nor a cli-passed config can be found."""


class MissingDbtProject(DbtCovesException):
    """Thrown when one or more in-scope dbt projects could not be found."""


class TargetNameNotProvided(DbtCovesException):
    """Thrown when no `target:` entry is provided in the profiles.yml and not passed on CLI."""


class KnownRegressionError(DbtCovesException):
    """Thrown when we want to warn users of a known regression or limitation that is not implemented."""


class MissingCommand(DbtCovesException):
    """Thrown when no command was specified and we want to show the help."""
