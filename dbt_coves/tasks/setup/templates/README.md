# Starter Project

Welcome to your Starter Project! This repository is designed to help users kickstart their journey with dbt (data build tool) and Airflow. Whether you're new to data modeling or an experienced data engineer, this repo will assist you in setting up dbt for your environment and tailoring it to your specific data needs.

## Brought by Datacoves

Datacoves is an [enterprise dbt DataOps platform](https://datacoves.com/product) which helps organizations overcome their data delivery challenges quickly using dbt and Airflow, implementing best practices from the start without the need for multiple vendors or costly consultants.

## Getting Started

To make use of this repo on your dbt journey, follow these steps:

1. **Configure your CICD**:

- Edit your gitlab-ci,yml or .github/workflows files based on your Data Warehouse and dbt project location. This will involve commenting and uncommenting lines of code in the files.
- For Gitlab users: Generate your Personal Access token.
  - Head to user > preferences > Access Tokens
  - Name the Token `GITLAB_PUSH_TOKEN`
  - Select the expiration data
  - Select api, read_repository, write_repository
  - Copy the token since it will not be viewable once you navigate from that screen
  - Configure the `GITLAB_PUSH_TOKEN` variable in your workflow environment. Settings > CICD > Variables. Be sure to select Masked for sensitive values.
- Configure your workflow environment variables in Gitlab or Github. See .gitlab-ci,yml or .github/workflows. Be sure to select Masked for sensitive values in gitlab or set secrets in github.

2. **Configure dbt**:

   - Configure dbt for your environment by editing the `profiles.yml` file in the `automate/dbt/` directory. Ensure you provide accurate connection details for your Data Warehouse.

3. **Customize Your Project**:

   - Define your data models in the `models` directory using SQL files. Organize your models according to your data warehouse schema and naming conventions.

4. **Run dbt**:

   - Execute your dbt transformations using the `dbt debug` command within the repository directory.
   - Execute your dbt transformations using the `dbt run` command within the repository directory. This will compile your SQL models and execute them against your warehouse.

## Resources

- [Datacoves](https://datacoves.com)
- [dbt docs](https://docs.getdbt.com)
- [Airflow docs](https://airflow.apache.org/docs/)
- [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint)
- [sqlfluff](https://github.com/sqlfluff/sqlfluff)

Happy modeling! 🚀
