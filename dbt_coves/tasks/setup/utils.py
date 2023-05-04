import os
from pathlib import Path

import git
from git.exc import InvalidGitRepositoryError
from rich.console import Console
from rich.table import Table

console = Console()

KEY_COLUMN_WIDTH = 50
VALUE_COLUMN_WIDTH = 30


def print_row(
    key,
    value="[green]FOUND :heavy_check_mark:[/green]",
    new_section=False,
    KEY_COLUMN_WIDTH=50,
    VALUE_COLUMN_WIDTH=30,
):
    grid = Table.grid(expand=False)
    grid.add_column(width=KEY_COLUMN_WIDTH)
    grid.add_column(justify="right", width=VALUE_COLUMN_WIDTH)
    grid.add_row(key, value)
    if new_section:
        console.print("\n")
    console.print(grid)


def file_exists(root_path, file_name):
    for path in Path(root_path).rglob(file_name):
        return path
    return False


def get_git_root(path=None):
    try:
        git_repo = git.Repo(path, search_parent_directories=True)
        git_root = git_repo.git.rev_parse("--show-toplevel")
        return git_root
    except InvalidGitRepositoryError:
        raise Exception(f"{path or 'current path'} doesn't belong to a git repository")


def get_dbt_projects(path=None):
    if not path:
        path = os.getcwd()
    dbt_project_dirs = []
    for file in Path(path).rglob("dbt_project.yml"):
        if "dbt_packages" not in str(file):
            dbt_project_dirs.append(str(file.relative_to(path).parent))
    return dbt_project_dirs
