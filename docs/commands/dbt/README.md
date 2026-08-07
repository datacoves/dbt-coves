## dbt-coves dbt

### Overview

`dbt-coves dbt` runs a dbt command on a special or restricted environment, such as an Airflow worker or a CI runner, where the dbt project may live on a read-only filesystem or need a specific Python virtual environment activated first.

```console
dbt-coves dbt <arguments> -- <dbt command>
```

If the target dbt project directory is read-only, dbt-coves transparently copies it to a writable temp directory and runs the dbt command there instead of failing. The path of that read-write clone is written to `/tmp/dbt_coves_dbt_clone_path.txt` and reused on subsequent invocations, so repeated runs (e.g. successive Airflow tasks in the same DAG run) don't re-copy the project each time. Use `--cleanup` to remove the clone once the command finishes instead of leaving it for reuse.

Before running the requested dbt command, `dbt-coves dbt` also checks whether `dbt_packages/` (or the legacy `dbt_modules/`) exists in the target directory, and runs `dbt deps` first if it's missing.

### Arguments

`dbt-coves dbt` supports the following args:

```console
--virtualenv
# Path to virtual environment where the dbt command will be executed, i.e. '/opt/user/virtualenvs/airflow'
# If omitted, dbt runs directly on PATH.
```

```console
--cleanup
# Flag: after running the command, delete the read-write clone created for a read-only project.
# Without this flag the clone is kept (and its path recorded) so later calls can reuse it.
```

```console
command
# Positional, required: the dbt command (and its own arguments) to run, e.g. 'run -s model_name'
```

### Discussion

- The dbt project directory itself is **not** a dedicated flag on this command - it comes from the shared `--project-dir` [global option](../README.md#global-options), the `dbt.project_dir` setting in `.dbt_coves.yml`, or (if neither is set) the `DBT_PROJECT_DIR` / `DATACOVES__DBT_HOME` environment variables, checked in that order. If none resolve to a value, the command fails with "No dbt project specified". `DATACOVES__DBT_HOME` is set automatically inside Datacoves-managed containers (code-server, Airflow workers/scheduler) - you don't need to set it yourself there.
- Everything after the mandatory `--` is passed straight through to `dbt` as-is, so any native dbt flag (`-s`, `--full-refresh`, `--vars`, etc.) works exactly as it would with a plain `dbt` invocation.
- This command is what Datacoves' Airflow DAGs use under the hood to run dbt tasks against a read-only, git-synced project checkout.

### Sample usage

```console
dbt-coves dbt --virtualenv /opt/user/virtualenvs/airflow --cleanup -- run -s model_name --vars "{key: value}"
# Make sure to escape special characters such as quotation marks
# The double dash (--) between <arguments> and <dbt command> is mandatory
```
