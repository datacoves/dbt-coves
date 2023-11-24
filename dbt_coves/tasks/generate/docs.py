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
        command = ["dbt", "docs", "generate"]
        if self.config.args.PROFILES_DIR:
            command.extend(["--profiles-dir", self.config.args.PROFILES_DIR])
        output = run_and_capture_cwd(command, self.config.project_root)

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
        with open(dbt_docs_index_path, "w+") as f:
            html_content = f.read()
            html_content.replace("</head>", "<base target='_blank'></head>")
            f.write(html_content)
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
        nodes_diff = set(target_catalog.get("nodes", {}).keys()) - set(
            local_catalog.get("nodes", {}).keys()
        )
        n_nodes_merged = len(nodes_diff)

        sources_diff = set(target_catalog.get("sources", {}).keys()) - set(
            local_catalog.get("sources", {}).keys()
        )
        n_sources_merged = len(sources_diff)

        local_catalog["nodes"].update(
            (key, value)
            for key, value in target_catalog.get("nodes", {}).items()
            if key not in local_catalog.get("nodes", {})
        )
        local_catalog["sources"].update(
            (key, value)
            for key, value in target_catalog.get("sources", {}).items()
            if key not in local_catalog.get("sources", {})
        )

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
        self._generate_dbt_docs()

        local_docs_path = Path(self.config.project_root, "target")
        self._fix_dbt_docs_links(local_docs_path)

        merge_deferred = self.get_config_value("merge_deferred")
        state_location = self.get_config_value("state")

        if merge_deferred:
            target_docs_path = Path(state_location, "catalog.json")
            if not state_location or not target_docs_path.exists():
                raise DbtCovesGenerateDocsException(
                    "A valid [red][i]--state[/i][/red] argument is required "
                    "when using [yellow]--merge-deferred[/yellow]"
                )

            self._merge_dbt_catalogs(local_docs_path, state_location)
        return 0
