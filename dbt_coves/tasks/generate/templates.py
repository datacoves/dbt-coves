import glob
import shutil
from pathlib import Path

from rich import console

import dbt_coves

from .base import BaseGenerateTask

console = console.Console()


class GenerateTemplatesException(Exception):
    pass


class GenerateTemplatesTask(BaseGenerateTask):
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

    def run(self):
        dbtcoves_config_folder = self.coves_config.get_config_folder()
        dbtcoves_templates_path = Path(dbt_coves.__file__).parent / "templates"

        templates_destination_path = Path(dbtcoves_config_folder / "templates")
        templates_destination_path.mkdir(parents=True, exist_ok=True)

        for dbtcoves_template in glob.glob(f"{dbtcoves_templates_path}/*.*"):
            template_path = Path(dbtcoves_template)
            template_name = template_path.name
            template_destination = templates_destination_path / template_name
            try:
                shutil.copyfile(template_path, template_destination)
                console.print(
                    f"Generated [green]{template_name}[/green] in {templates_destination_path.relative_to(Path.cwd())}"
                )
            except OSError as e:
                raise GenerateTemplatesException(
                    f"Couldn't generate {template_name} template file: {e}"
                )
        return 0
