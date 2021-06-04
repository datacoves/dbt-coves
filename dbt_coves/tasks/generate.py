
import questionary
from shutil import copytree
from pathlib import Path

from questionary import Choice
from rich.console import Console

console = Console()

SOURCE_PATH = "/config/dbt-coves/samples"
TARGET_PATH = "/config/workspace/models/staging/cdc_covid"


class GenerateTask:
    def run(self) -> int:
        asset = self.which_asset()
        if asset == "Sources":
            self.choose_sources()
            self.flatten_columns()
            console.print("\n"
                          "SQL and YML files were successfully generated under [u]models/sources/cdc_covid[/u] for these sources:\n\n"
                          " - [bold magenta]cases_deaths_daily_usa[/bold magenta]\n"
                          " - [bold magenta]vaccines_pfizer[/bold magenta]\n"
                          " - [bold magenta]vaccines_janssen[/bold magenta]\n"
                          " - [bold magenta]vaccines_moderna[/bold magenta]\n")
            # Path(TARGET_PATH).mkdir(parents=True, exist_ok=True)
            copytree(SOURCE_PATH, TARGET_PATH, dirs_exist_ok=True)
        return 0

    def which_asset(self):
        asset = questionary.select(
            "What would you like to generate?",
            choices=[
                "Sources",
                "Marts",
                "Docs",
                "Tests"
            ],
            default="Sources").ask()
        return asset

    def choose_sources(self):
        sources = questionary.checkbox(
            "Which sources would you like to generate?",
            choices=[
                Choice("[cdc_covid] cases_deaths_daily_usa", checked=True),
                Choice("[cdc_covid] vaccines_pfizer", checked=True),
                Choice("[cdc_covid] vaccines_janssen", checked=True),
                Choice("[cdc_covid] vaccines_moderna", checked=True)
            ]).ask()
        return sources

    def flatten_columns(self):
        flattened = questionary.confirm(
            "Variant columns detected. Would you like to flatten them?",
            default=True).ask()
        return flattened
