# Models generation

## Overview

The `dbt-coves generate` command allows you to generate different types of resources based on your project's needs. These resources can include data sources, properties, metadata, dbt docs, and even Airflow DAGs for scheduling and orchestrating your data pipelines.

By leveraging this command, you can quickly bootstrap new projects, create boilerplate code, and maintain a consistent structure across your data engineering projects. This not only improves productivity, but also promotes code reusability and maintainability.

## Usage

The general syntax for the `dbt-coves generate` command is as follows:

```console
dbt-coves generate <resource>
```

Where `resource` could be:

- [_airflow-dags_](airflow%20dags/): generate Airflow DAGs for orchestration
- [_docs_](docs/): generate dbt docs
- [_metadata_](metadata/): generate metadata for your database table(s)
- [_properties_](properties/): generate sources' YML schemas
- [_sources_](sources/): generate dbt sources
- [_templates_](templates/): generate dbt-coves config folder and templates
