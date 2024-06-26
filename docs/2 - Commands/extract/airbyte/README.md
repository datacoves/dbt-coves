## dbt-coves extract airbyte

Extracts the configuration from your Airbyte sources, connections and destinations (excluding credentials) and stores it in the specified folder. The main goal of this feature is to keep track of the configuration changes in your git repo, and rollback to a specific version when needed.

```shell
dbt-coves extract airbyte <arguments>
```

### Arguments

`dbt-coves extract airbyte` supports the following arguments

```shell
--path
# Path where configuration json files will be created, i.e. '/var/data/airbyte_extract/'
```

```shell
--host
# Airbyte's API hostname, i.e. 'http://airbyte-server'
```

```shell
--port
# Airbyte's API port, i.e. '8001'
```

### Sample usage

```shell
dbt-coves extract airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load/airbyte
```
