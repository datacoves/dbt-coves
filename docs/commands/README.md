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

Throughout the command documentation, you'll find usage examples that demonstrate how to use each command and its various options. These examples are designed to help you understand the command's functionality and provide a starting point for incorporating it into your data engineering workflows.

## Contributing

If you find any issues or have suggestions for improving the command documentation, please refer to the [Contributing Guidelines](../contributing.md) for information on how to submit your feedback or contributions.
