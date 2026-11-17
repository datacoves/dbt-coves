"""
--vars is passed to dbt's RuntimeConfig, which since dbt 1.8 expects the YAML already
parsed into a dict (dbt's own CLI parses it). dbt-coves passed the raw string, so any
--vars crashed with "TypeError: 'str' object is not a mapping".
"""

import argparse

import pytest

from dbt_coves.core.main import DBT_PARSES_CLI_VARS, base_subparser
from dbt_coves.tasks.generate.main import GenerateTask
from dbt_coves.utils.flags import DbtCovesFlags

pytestmark = pytest.mark.skipif(
    DBT_PARSES_CLI_VARS, reason="dbt < 1.8 parses the --vars string itself"
)


def parse(*cli_args):
    # main's parser only registers the subcommand found in sys.argv, so build one
    # the same way with `generate` registered.
    parser = argparse.ArgumentParser(prog="dbt-coves")
    sub_parsers = parser.add_subparsers(title="dbt-coves commands", dest="task")
    GenerateTask.register_parser(sub_parsers, base_subparser)
    flags = DbtCovesFlags(parser)
    flags.parse_args(["generate", "properties", *cli_args])
    return flags.args


@pytest.fixture
def project(tmp_path):
    (tmp_path / "dbt_project.yml").write_text(
        "name: vars_test\n"
        "version: '1.0'\n"
        "profile: vars_test\n"
        "models:\n"
        "  vars_test:\n"
        "    +schema: \"{{ var('schema_suffix') }}\"\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "vars_test:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        "      account: acct\n"
        "      user: user\n"
        "      password: password\n"
        "      database: db\n"
        "      schema: sch\n"
        "      warehouse: wh\n"
    )
    return tmp_path


def test_vars_are_parsed_into_a_dict():
    assert parse("--vars", "{schema_suffix: x, n: 1}").vars == {
        "schema_suffix": "x",
        "n": 1,
    }


def test_vars_default_is_an_empty_dict():
    assert parse().vars == {}


@pytest.mark.parametrize("bad", ["[1, 2]", "just a string", "{unclosed: [1"])
def test_invalid_vars_are_a_usage_error(bad, capsys):
    with pytest.raises(SystemExit) as exc:
        parse("--vars", bad)
    assert exc.value.code == 2
    assert "--vars" in capsys.readouterr().err


def test_dbt_runtime_config_accepts_parsed_vars(project):
    from dbt.config.runtime import RuntimeConfig
    from dbt.flags import set_from_args
    from dbt_common.clients.system import get_env
    from dbt_common.context import set_invocation_context

    args = parse(
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--vars",
        "{schema_suffix: from_cli}",
    )
    # The same steps BaseConfiguredTask.from_args takes.
    set_from_args(args, None)
    set_invocation_context(get_env())
    config = RuntimeConfig.from_args(args)

    assert config.cli_vars == {"schema_suffix": "from_cli"}
