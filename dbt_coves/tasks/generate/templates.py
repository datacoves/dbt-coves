import glob
import shutil
from pathlib import Path

import questionary
from rich import console

import dbt_coves
from dbt_coves.tasks.base import BaseConfiguredTask

console = console.Console()


class GenerateTemplatesException(Exception):
    pass


class GenerateTemplatesTask(BaseConfiguredTask):
    """
    Task that generates dbt-coves templates on coves-config folder
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "templates", help="Generate dbt-coves templates on .dbt-coves config folder"
        )

        cls.arg_parser = base_subparser
        subparser.set_defaults(cls=cls, which="templates")
        return subparser

    def copy_template_file(self, template_name, dbtcoves_template_path, destination_template_path):
        try:
            shutil.copyfile(dbtcoves_template_path, destination_template_path)
            console.print(f"Generated [green]{destination_template_path}[/green]")
        except OSError as e:
            raise GenerateTemplatesException(
                f"Couldn't generate {template_name} template file: {e}"
            )

    def run(self):
        options = {"overwrite_all": False}
        dbtcoves_templates_path = Path(dbt_coves.__file__).parent / "templates"

        dbt_project_path = self.config.project_root
        templates_destination_path = (
            dbt_project_path
            / Path(self.coves_config.DBT_COVES_CONFIG_FILEPATH).parent
            / "templates"
        )
        templates_destination_path.mkdir(parents=True, exist_ok=True)

        for dbtcoves_template in glob.glob(f"{dbtcoves_templates_path}/*.*"):
            dbtcoves_template_path = Path(dbtcoves_template)
            template_name = dbtcoves_template_path.name
            target_destination = templates_destination_path / template_name
            if target_destination.exists():
                if not options["overwrite_all"]:
                    console.print(f"[yellow]{target_destination}[/yellow] already exists.")
                    overwrite = questionary.select(
                        f"Would you like to overwrite it?",
                        choices=["No", "Yes", "Overwrite all", "Cancel"],
                    ).ask()
                    if overwrite == "No":
                        continue
                    if overwrite == "Overwrite all":
                        options["overwrite_all"] = True
                    if overwrite == "Cancel":
                        exit()

            self.copy_template_file(template_name, dbtcoves_template_path, target_destination)

        return 0
