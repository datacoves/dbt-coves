## dbt-coves dbt

This dbt-coves command allows us to run dbt commands on special environments such as Airflow, or CI workers, with the possibility of changing dbt project location and activating a specific virtual environment in which running the desired command.

If the project directory is read-only (widely seen in Airflow projects), it is copied to a temporary folder to perform the desired execution.

```shell
dbt-coves dbt <arguments> -- <command>
```

### Arguments

`dbt-coves dbt` supports the following arguments

```shell
--project-dir
# Path of the dbt project where command will be executed, i.e.: /opt/user/dbt_project
```

```shell
--virtualenv
# Virtual environment path. i.e.: /opt/user/virtualenvs/airflow
```

### Sample usage

```shell
dbt-coves dbt --project-dir /opt/user/dbt_project --virtualenv /opt/user/virtualenvs/airflow -- run -s model --vars \"{key: value}\"
# Make sure to escape special characters such as quotation marks
# Double dash (--) between <arguments> and <command> are mandatory
```
