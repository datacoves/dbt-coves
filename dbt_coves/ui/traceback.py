"""Holds handlers to manage error tracebacks using."""
import pretty_errors

from dbt_coves.utils.flags import DbtCovesFlags


class DbtCovesTraceback:
    """Consumes CLI flags (from DbtCovesFlags consumer) and sets up traceback pretty formatting."""

    def __init__(self, flags: DbtCovesFlags) -> None:
        """Traceback constructor.

        Consumes flags from the DbtCovesFlags objects and sets up traceback formatting so that we
        can print prettier errors.

        Args:
            flags (DbtCovesFlags): [description]
        """
        stack_depth: int = 1
        if flags.verbose:
            stack_depth = 10

        pretty_errors.configure(
            separator_character="*",
            line_number_first=False,
            display_link=True,
            lines_before=5,
            lines_after=2,
            line_color=pretty_errors.RED + "> " + pretty_errors.default_config.line_color,
            code_color="  " + pretty_errors.default_config.line_color,
            truncate_code=True,
            display_locals=True,
            stack_depth=stack_depth,
            trace_lines_before=4,
            trace_lines_after=4,
            display_arrow=True,
        )
