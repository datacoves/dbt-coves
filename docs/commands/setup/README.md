## Environment setup

You can configure different components:

Set up `git` repository of dbt-coves project

```console
dbt-coves setup git
```

Set up `dbt` within the project (delegates to dbt init)

```console
dbt-coves setup dbt
```

Set up SSH Keys for dbt project. Supports the argument `--open_ssl_public_key` which generates an extra Public Key in Open SSL format, useful for configuring certain providers (i.e. Snowflake authentication)

```console
dbt-coves setup ssh
```

Set up pre-commit for your dbt project. In this, you can configure different tools that we consider essential for proper dbt usage: `sqlfluff`, `yaml-lint`, and `dbt-checkpoint`

```console
dbt-coves setup precommit
```
