import shutil
import os

from cookiecutter.main import cookiecutter
from rich.console import Console

from .base import BaseTask

console = Console()


class InitTask(BaseTask):
    """
    Task that clones and applies a cookiecutter template
    """
    
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "init",
            parents=[base_subparser],
            help="Initializes a new dbt project using predefined conventions.",
        )
        subparser.add_argument(
            "--template",
            type=str,
            help="Cookiecutter template github url, i.e."
                 " 'https://github.com/datacoves/cookiecutter-dbt-coves.git'",
        )
        subparser.add_argument(
            "--current-dir",
            help="Generate the dbt project in the current directory.",
            action="store_true",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="init")
        return subparser

    def run(self) -> int:
        if self.coves_flags.init["current-dir"]:
            current_files = set(os.listdir('.')) - set(['logs'])
            if current_files:
                console.print('Current dir needs to be empty if using --current-dir argument.')
                return 0

        template_url = self.coves_flags.init["template"]
        console.print(f"Applying cookiecutter template {template_url} to your project...\n")
        new_dir = cookiecutter(template_url)

        if self.coves_flags.init["current-dir"]:
            file_names = os.listdir(new_dir)
            for file_name in file_names:
                shutil.move(os.path.join(new_dir, file_name), '.')
            os.rmdir(new_dir)

        return 0
