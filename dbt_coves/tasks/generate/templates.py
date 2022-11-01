from pathlib import Path

import requests
from rich import console

from .base import BaseGenerateTask

console = console.Console()


class GenerateTemplatesException(Exception):
    pass


class GenerateTemplatesTask(BaseGenerateTask):
    """
    Task that generates dbt-coves templates on coves-config folder
    """

    DBT_COVES_TEMPLATES = [
        "model_props.yml",
        "source_props.yml",
        "staging_model.sql",
        "staging_model_props.yml",
    ]

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "templates", help="Generate dbt-coves templates on .dbt-coves config folder"
        )

        cls.arg_parser = base_subparser
        subparser.set_defaults(cls=cls, which="templates")
        return subparser

    def run(self):
        coves_config_folder = self.coves_config.get_config_folder()
        templates_dest_path = Path(coves_config_folder / "templates")
        templates_dest_path.mkdir(parents=True, exist_ok=True)

        for template in self.DBT_COVES_TEMPLATES:
            template_path = templates_dest_path / template
            template_req = requests.get(
                f"https://raw.githubusercontent.com/datacoves/dbt-coves/main/dbt_coves/templates/{template}"
            )
            if template_req.status_code == 200:
                with open(template_path, "w") as file:
                    file.write(template_req.text)
                console.print(
                    f"Generated [green]{template}[/green] in {templates_dest_path.relative_to(Path.cwd())}"
                )

            else:
                raise GenerateTemplatesException(
                    f"There was an error getting {template} template: {template_req.text}"
                )

        return 0
