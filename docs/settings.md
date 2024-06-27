# Settings

dbt-coves will read settings from `.dbt_coves/config.yml`. A standard settings files could look like this:

```yaml
generate:
  sources:
    database: "RAW" # Database where to look for source tables
    schemas: # List of schema names where to look for source tables
      - RAW
    select_relations: # list of relations where raw data resides
      - TABLE_1
      - TABLE_2
    exclude_relations: # Filter relation(s) to exclude from source file(s) generation
      - TABLE_1
      - TABLE_2
    sources_destination: "models/staging/{{schema}}/{{schema}}.yml" # Where sources yml files will be generated
    models_destination: "models/staging/{{schema}}/{{relation}}.sql" # Where models sql files will be generated
    model_props_destination: "models/staging/{{schema}}/{{relation}}.yml" # Where models yml files will be generated
    update_strategy: ask # Action to perform when a property file already exists. Options: update, recreate, fail, ask (per file)
    templates_folder: ".dbt_coves/templates" # Folder where source generation jinja templates are located. Override default templates creating  source_props.yml, source_model_props.yml, and source_model.sql under this folder
    metadata: "metadata.csv" # Path to csv file containing metadata
    flatten_json_fields: ask

  properties:
    destination: "{{model_folder_path}}/{{model_file_name}}.yml" # Where models yml files will be generated
    # You can specify a different path by declaring it explicitly, i.e.: "models/staging/{{model_file_name}}.yml"
    update-strategy: ask # Action to perform when a property file already exists. Options: update, recreate, fail, ask (per file)
    select: "models/staging/bays" # Filter model(s) to generate property file(s)
    exclude: "models/staging/bays/test_bay" # Filter model(s) to generate property file(s)
    selector: "selectors/bay_selector.yml" # Specify dbt selector for more complex model filtering
    templates_folder: ".dbt_coves/templates" # Folder where source generation jinja templates are located. Override default template creating model_props.yml under this folder
    metadata: "metadata.csv" # Path to csv file containing metadata

  metadata:
    database: RAW # Database where to look for source tables
    schemas: # List of schema names where to look for source tables
      - RAW
    select_relations: # list of relations where raw data resides
      - TABLE_1
      - TABLE_2
    exclude_relations: # Filter relation(s) to exclude from source file(s) generation
      - TABLE_1
      - TABLE_2
    destination: # Where metadata file will be generated, default: 'metadata.csv'

  docs:
    merge_deferred: true
    state: logs/
    dbt_args: "--no-compile --select foo --exclude bar"

  airflow_dags:
    yml_path:
    dags_path:
    generators_params:
      AirbyteDbtGenerator:
        host: "{{ env_var('AIRBYTE_HOST_NAME') }}"
        port: "{{ env_var('AIRBYTE_PORT') }}"
        airbyte_conn_id: airbyte_connection

        dbt_project_path: "{{ env_var('DBT_HOME') }}"
        run_dbt_compile: true
        run_dbt_deps: false

extract:
  airbyte:
    path: /config/workspace/load/airbyte # Where json files will be generated
    host: http://airbyte-server # Airbyte's API hostname
    port: 8001 # Airbyte's API port
  fivetran:
    path: /config/workspace/load/fivetran # Where Fivetran export will be generated
    api_key: [KEY] # Fivetran API Key
    api_secret: [SECRET] # Fivetran API Secret
    credentials: /opt/fivetran_credentials.yml # Fivetran set of key:secret pairs
    # 'api_key' + 'api_secret' are mutually exclusive with 'credentials', use one or the other

load:
  airbyte:
    path: /config/workspace/load
    host: http://airbyte-server
    port: 8001
    secrets_manager: datacoves # (optional) Secret credentials provider (secrets_path OR secrets_manager should be used, can't load secrets locally and remotely at the same time)
    secrets_path: /config/workspace/secrets # (optional) Secret files location if secrets_manager was not specified
    secrets_url: https://api.datacoves.localhost/service-credentials/airbyte # Secrets url if secrets_manager is datacoves
    secrets_token: <TOKEN> # Secrets auth token if secrets_manager is datacoves
  fivetran:
    path: /config/workspace/load/fivetran # Where previous Fivetran export resides, subject of import
    api_key: [KEY] # Fivetran API Key
    api_secret: [SECRET] # Fivetran API Secret
    secrets_path: /config/workspace/secrets/fivetran # Fivetran secret fields
    credentials: /opt/fivetran_credentials.yml # Fivetran set of key:secret pairs
    # 'api_key' + 'api_secret' are mutually exclusive with 'credentials', use one or the other
```

## env_var

From `dbt-coves 1.6.28` onwards, you can consume environment variables in you config file using `"{{env_var('VAR_NAME', 'DEFAULT VALUE')}}"`. For example:

```yaml
generate:
  sources:
    database: "{{env_var('MAIN_DATABASE', 'dev_database')}}"
    schemas:
      - "{{env_var('DEV_SCHEMA', 'John')}}"
      - "{{env_var('STAGING_SCHEMA', 'Staging')}}"
```

## Telemetry

dbt-coves has telemetry built in to help the maintainers from Datacoves understand which commands are being used and which are not to prioritize future development of dbt-coves. We do not track credentials nor details of your dbt execution such as model names. The one detail we do use related to dbt is the anonymous user_id to help us identify distinct users.

By default this is turned on – you can opt out of event tracking at any time by adding the following to your dbt-coves `config.yaml` file:

```yaml
disable-tracking: true
```

## Override generation templates

Customizing generated models and model properties requires placing
template files under the `.dbt-coves/templates` folder.

There are different variables available in the templates:

- `adapter_name` refers to the Adapter's class name being used by the target, e.g. `SnowflakeAdapter` when using [Snowflake](https://github.com/dbt-labs/dbt-snowflake/blob/21b52127e7d221db8b92114aae066fb8a7151bba/dbt/adapters/snowflake/impl.py#L33).
- `columns` contains the list of relation columns that don't contain nested (JSON) data, it's type is `List[Item]`.
- `nested` contains a dict of nested columns, grouped by column name, it's type is `Dict[column_name, Dict[nested_key, Item]]`.

`Item` is a `dict` with the keys `id`, `name`, `type`, and `description`, where `id` contains an slugified id generated from `name`.

### dbt-coves generate sources

#### Source property file (.yml) template

This file is used to create the sources yml file

[source_props.yml](dbt_coves/templates/source_props.yml)

#### Staging model file (.sql) template

This file is used to create the staging model (sql) files.

[staging_model.sql](dbt_coves/templates/staging_model.sql)

#### Staging model property file (.yml) template

This file is used to create the model properties (yml) file

[staging_model_props.yml](dbt_coves/templates/staging_model_props.yml)

### dbt-coves generate properties

This file is used to create the properties (yml) files for models

[model_props.yml](dbt_coves/templates/model_props.yml)
