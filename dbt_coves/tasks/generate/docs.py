from __future__ import nested_scopes

import json
from pathlib import Path
from typing import Any, Dict

from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.tasks.setup.utils import print_row
from dbt_coves.utils.shell import run_and_capture_cwd
from dbt_coves.utils.tracking import trackable

console = Console()


class DbtCovesGenerateDocsException(Exception):
    pass


class GenerateDocsTask(BaseConfiguredTask):
    """
    Task that generates content on local catalog.json
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "docs",
            parents=[base_subparser],
            help="Merge models from a catalog.json into another one.",
        )
        subparser.add_argument(
            "--merge-deferred",
            action="store_true",
            help="Flag to merge deferred models and sources into local catalog",
            default=False,
        )
        subparser.add_argument(
            "--state",
            type=str,
            help="Catalog.json to use as reference for merging",
        )
        cls.arg_parser = base_subparser
        subparser.set_defaults(cls=cls, which="docs")
        return subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["docs"][key]

    def _generate_dbt_docs(self):
        output = run_and_capture_cwd(["dbt", "docs", "generate"], self.config.project_root)

        if output.returncode == 0:
            deps_status = "[green]SUCCESS :heavy_check_mark:[/green]"
        else:
            deps_status = "[red]FAIL :cross_mark:[/red]"
        print_row(
            "dbt docs generate",
            deps_status,
            new_section=True,
        )
        if output.returncode > 0:
            raise Exception("dbt deps error. Check logs.")

    def _fix_dbt_docs_links(self, docs_path: Path):
        dbt_docs_index_path = Path(docs_path, "index.html")
        new_tab_tag = "<base target='_blank'>"
        with open(dbt_docs_index_path, "r") as f:
            html_content = f.read()

        head_start = html_content.find("<head>") + len("<head>")
        head_end = html_content.find("</head>")

        if new_tab_tag not in html_content[head_start:head_end]:
            modified_content = html_content[:head_end] + new_tab_tag + html_content[head_end:]

            # Write the modified content back to the HTML file
            with open(dbt_docs_index_path, "w") as html_file:
                html_file.write(modified_content)
            console.print(
                "[green]:heavy_check_mark:[/green] dbt docs updated. "
                "External links will now open in a new tab"
            )

    def _get_catalog_json(self, docs_folder: Path) -> Dict[str, Any]:
        """
        Open json at docs_folder/catalog.json
        """
        catalog_path = Path(docs_folder, "catalog.json")
        try:
            with open(catalog_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            raise DbtCovesGenerateDocsException(f"Catalog.json not found at {catalog_path}")

    def _merge_catalogs(
        self, local_catalog: Dict[str, Any], target_catalog: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge nodes and sources from the state catalog.json into the local docs.
        """
        n_nodes_merged = 0
        n_sources_merged = 0

        for key, value in target_catalog.get("nodes", {}).items():
            if key not in local_catalog.get("nodes", {}):
                local_catalog["nodes"][key] = value
                n_nodes_merged += 1

        for key, value in target_catalog.get("sources", {}).items():
            if key not in local_catalog.get("sources", {}):
                local_catalog["sources"][key] = value
                n_sources_merged += 1

        console.print(
            f"Merged [green]{n_nodes_merged} nodes[/green] and [green]{n_sources_merged} sources[/green] into",
            "your local catalog.json",
        )

    def _write_catalog_json(self, catalog: Dict[str, Any], docs_folder: Path):
        """
        Write the catalog.json to the docs_folder
        """
        catalog_path = Path(docs_folder, "catalog.json")
        with open(catalog_path, "w") as f:
            json.dump(catalog, f)

    def _merge_dbt_catalogs(self, local_path: Path, state_path: Path):
        """
        Merge the catalog.json from the stsate into the local docs.
        """
        # Get the source and target catalogs
        local_catalog = self._get_catalog_json(local_path)
        target_catalog = self._get_catalog_json(state_path)

        # Merge the catalogs
        self._merge_catalogs(local_catalog, target_catalog)

        # Write the merged catalog to the source
        self._write_catalog_json(local_catalog, local_path)

    @trackable
    def run(self):
        # self._generate_dbt_docs()

        local_docs_path = Path(self.config.project_root, "target")
        self._fix_dbt_docs_links(local_docs_path)

        merge_deferred = self.get_config_value("merge_deferred")
        state_location = self.get_config_value("state")

        if merge_deferred:
            target_docs_path = Path(state_location, "catalog.json")
            if not state_location or not target_docs_path.exists():
                console.print(
                    "A valid [red][i]--state[/i][/red] argument is required "
                    "when using [yellow]--merge-deferred[/yellow]"
                )
                return -1
            self._merge_dbt_catalogs(local_docs_path, state_location)
        return 0
