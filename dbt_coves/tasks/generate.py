
import questionary
from questionary import Choice
from rich.console import Console

console = Console()


class GenerateTask:
    def run(self) -> int:
        asset = self.which_asset()
        if asset == "Sources":
            self.choose_sources()
            self.flatten_columns()
            console.print("\n"
                          "Models [bold magenta]cases_deaths_daily_usa.sql[/bold magenta], [bold magenta]covid_vaccines_pfizer.sql[/bold magenta], "
                          "[bold magenta]covid_vaccines_janssen.sql[/bold magenta], and [bold magenta]covid_vaccines_moderna.sql[/bold magenta] "
                          "were successfully generated under [u]models/sources/cdc_covid[/u].")

        console.print("\n")
        return 0

    def which_asset(self):
        asset = questionary.select(
            "What would you like to generate?",
            choices=[
                "Sources",
                "Bays",
                "Coves",
                "Docs",
                "Tests"
            ],
            default="Sources").ask()
        return asset

    def choose_sources(self):
        sources = questionary.checkbox(
            "Which sources would you like to generate?",
            choices=[
                Choice("[CDC_COVID] CASES_DEATHS_DAILY_USA", checked=True),
                Choice("[CDC_COVID] COVID_VACCINES_PFIZER", checked=True),
                Choice("[CDC_COVID] COVID_VACCINES_JANSSEN", checked=True),
                Choice("[CDC_COVID] COVID_VACCINES_MODERNA", checked=True)
            ]).ask()
        return sources

    def flatten_columns(self):
        flattened = questionary.confirm(
            "Variant columns detected. Would you like to flatten them?",
            default=True).ask()
        return flattened
