## dbt-coves setup

### Overview

```console
dbt-coves setup
```

Scaffolds (or updates) the standard Datacoves project components - dbt project skeleton, CI configuration, pre-commit hooks, and Airflow DAGs - into the current repository. Under the hood this runs [copier](https://copier.readthedocs.io/) against a template repository, so it behaves like `copier copy`/`copier update`: it prompts for any template variables it doesn't already know, and either writes new files (first run) or reconciles changes against a previous run (`--update`).

The destination is always the repository root, resolved from the `DATACOVES__REPO_PATH` environment variable when running inside a Datacoves environment, or the current working directory otherwise. `DATACOVES__REPO_PATH` is set automatically by Datacoves in code-server and CI containers - you don't need to set it yourself there.

### Arguments

`dbt-coves setup` supports the following args:

```console
--template-url
# URL to the setup template repository (git URL or local path).
# Default: https://github.com/datacoves/setup_template.git
```

```console
--update
# Flag: reconcile the existing generated files against the template instead of copying fresh.
# Use this to pick up template changes on a project that has already run `dbt-coves setup` once.
```

```console
--no-prompt
# Flag: generate all components without prompting for confirmation/values, accepting template defaults.
```

```console
--quiet
# Flag: suppress the copier diff/render output.
```

### Discussion

- `--update` and a plain (first-run) invocation are mutually exclusive modes internally: `--update` calls `copier.run_update`, anything else calls `copier.run_copy`. Don't pass `--update` on a directory that was never set up with this command - there's nothing for copier to diff against.
- dbt-coves also fetches the latest release tags for [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) and [yamllint](https://github.com/adrienverge/yamllint) from GitHub and exposes them to the template as `DATACOVES__DBT_CHECKPOINT_VERSION` / `DATACOVES__YAMLLINT_VERSION`, so generated pre-commit configs pin current versions without you having to look them up.
- The template also receives the dbt core/adapter version dbt-coves was installed against (`dbt_core_version`, `dbt_adapter_version`), so generated `dbt_project.yml`/CI config can stay consistent with the installed dbt.
- Both `--template-url` and `--update` can also be set via `setup.template_url` / `setup.update` in `.dbt_coves/config.yml`, but since this command is normally the very first one run in a fresh repo (before a config file necessarily exists), passing them as flags is more common in practice.

### Sample usage

```console
# First run in a new repo - scaffold everything, prompting for values
dbt-coves setup

# Non-interactive scaffold, e.g. inside an automated project bootstrap
dbt-coves setup --no-prompt --quiet

# Pull in template changes on a project set up previously
dbt-coves setup --update

# Use a custom/internal fork of the setup template
dbt-coves setup --template-url git@github.com:my-org/custom_setup_template.git
```
