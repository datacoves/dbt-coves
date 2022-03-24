
dbt-coves
*********

|Maintenance| |PyPI version fury.io| |Code Style| |Checked with mypy| |Imports: isort| |Imports: python| |Build| |pre-commit.ci status| |codecov| |Maintainability| |Downloads|

.. |Maintenance| image:: https://img.shields.io/badge/Maintained%3F-yes-green.svg
   :target: https://github.com/datacoves/dbt-coves/graphs/commit-activity

.. |PyPI version fury.io| image:: https://badge.fury.io/py/dbt-coves.svg
   :target: https://pypi.python.org/pypi/dbt-coves/

.. |Code Style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/ambv/black

.. |Checked with mypy| image:: http://www.mypy-lang.org/static/mypy_badge.svg
   :target: http://mypy-lang.org

.. |Imports: isort| image:: https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336
   :target: https://pycqa.github.io/isort/

.. |Imports: python| image:: https://img.shields.io/badge/python-3.8%20%7C%203.9-blue
   :target: https://img.shields.io/badge/python-3.8%20%7C%203.9-blue

.. |Build| image:: https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg
   :target: https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg

.. |pre-commit.ci status| image:: https://results.pre-commit.ci/badge/github/bitpicky/dbt-coves/main.svg
   :target: https://results.pre-commit.ci/latest/github/datacoves/dbt-coves/main

.. |codecov| image:: https://codecov.io/gh/datacoves/dbt-coves/branch/main/graph/badge.svg?token=JB0E0LZDW1
   :target: https://codecov.io/gh/datacoves/dbt-coves

.. |Maintainability| image:: https://api.codeclimate.com/v1/badges/1e6a887de605ef8e0eca/maintainability
   :target: https://codeclimate.com/github/datacoves/dbt-coves/maintainability

.. |Downloads| image:: https://pepy.tech/badge/dbt-coves
   :target: https://pepy.tech/project/dbt-coves

What is dbt-coves?
==================

dbt-coves is a complimentary CLI tool for `dbt <https://www.getdbt.com>`_ that allows users to quickly apply `Analytics Engineering <https://www.getdbt.com/what-is-analytics-engineering/>`_ best practices.

dbt-coves helps with the generation of scaffold for dbt by analyzing your data warehouse schema in Redshift, Snowflake, or Big Query and creating the necessary configuration files (sql and yml).

⚠️ **dbt-coves is in alpha version. Don’t use on your prod models unless you have tested it before.**

Here's the tool in action
-------------------------

.. image:: https://cdn.loom.com/sessions/thumbnails/74062cf71cbe4898805ca508ea2d9455-1624905546029-with-play.gif
   :target: https://www.loom.com/share/74062cf71cbe4898805ca508ea2d9455

Supported dbt versions
======================

.. list-table::
   :header-rows: 1

   * - Version
     - Status
   * - <= 0.17.0
     - ❌ Not supported
   * - 0.18.x - 0.21x
     - ✅ Tested
   * - 1.x
     - ✅ Tested

Supported adapters
==================

.. list-table::
   :header-rows: 1

   * - Feature
     - Snowflake
     - Redshift
     - BigQuery
     - Postgres
   * - profile.yml generation
     - ✅ Tested
     - 🕥 In progress
     - ❌ Not tested
     - ❌ Not tested
   * - sources generation
     - ✅ Tested
     - 🕥 In progress
     - ❌ Not tested
     - ❌ Not tested
