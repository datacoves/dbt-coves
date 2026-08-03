# Commands

## Command Structure

`dbt-coves` commands follow a hierarchical structure. Each top-level command may have one or more subcommands, and some subcommands may have further nested subcommands.

For example, `dbt-coves generate` command is a top-level command, while `dbt-coves generate sources` is a subcommand of `generate`.

## Command Documentation

The documentation for each command is organized into separate folders within the `commands` directory. Each folder represents a top-level command, and any subfolders within it represent subcommands.

For instance, the documentation for the `dbt-coves generate` command and its subcommands can be found in the `generate` folder:

- `generate/README.md`: Documentation for the `dbt-coves generate` command.
- `generate/sources/README.md`: Documentation for the `dbt-coves generate sources` subcommand.

This structure allows you to easily navigate and find the documentation for the specific command or subcommand you need.

## Usage Examples

Throughout the command documentation, you'll find usage examples that demonstrate how to use each command and its various options. These examples are designed to help you understand the command's functionality and provide a starting point for incorporating it into your data engineering workflow.

## Global Options

Every `dbt-coves` command accepts the following flags, in addition to the ones documented on its own page. Most of these mirror `dbt`'s own global flags, since dbt-coves shares its argument parser with several dbt-aware tasks.

```console
--config-path
# Full path to .dbt_coves.yml, if not using the default (current working directory)
```

```console
--project-dir
# Directory to look in for dbt_project.yml. Default: current working directory and its parents
```

```console
--profiles-dir
# Directory to look in for profiles.yml
```

```console
--profile
# Which dbt profile to load. Overrides the setting in dbt_project.yml
```

```console
-t, --target
# Which target to load for the given profile
```

```console
--vars
# Supply variables to dbt_project.yml as a YAML string, e.g. '{my_variable: my_value}'
```

```console
--threads
# Number of threads to use. Overrides the setting in profiles.yml
```

```console
--log-level
# Overrides the default log level
```

```console
-vv, --verbose
# Don't truncate tracebacks
```

```console
--disable-tracking
# Disable anonymous command usage tracking. No user information is ever collected.
```

The remaining global flags (`--version-check`, `--target-path`, `--log-path`, `--log-cache-events`, `--send-anonymous-usage-stats`, `--partial-parse`, `--partial-parse-file-diff`, `--static-parser`, `--require-names-without-spaces`, `--indirect-selection`) exist mainly so dbt-coves can pass dbt-flavored configuration through to the dbt-core adapter it loads internally; run `dbt-coves <command> -h` for the full, current list.

Any of these can also be set via `.dbt_coves.yml` (see [Settings](../settings.md)); a value passed on the command line always wins over the config file.

## Contributing

If you find any issue or have suggestions for improving the command documentation, please refer to the [Contributing Guidelines](../contributing.md) for information on how to submit your feedback or contributions.
