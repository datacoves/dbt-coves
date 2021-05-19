
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
                "Sources [bold magenta]salesforce[/bold magenta] and [bold magenta]exchange_rates[/bold magenta] "
                "were generated successfully under [u]models/sources[/u].")

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
        sources = questionary.select(
            "Which sources would you like to generate?",
            choices=[
                "Salesforce",
                "Exchange Rates",
                "All of them"
            ],
            default="All of them").ask()
        return sources

    def flatten_columns(self):
        flattened = questionary.checkbox(
            "Select sources for which you want JSON columns flattened?",
            choices=[
                Choice(title="Salesforce (no JSON)", disabled=True),
                Choice(title="Exchange Rates")
            ]).ask()
        return flattened
