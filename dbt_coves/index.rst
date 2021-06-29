Supported dbt versions
======================

.. list-table::
   :header-rows: 1

   * - Version
     - Status
   * - 0.17.0
     - ❌ Not supported
   * - 0.18.2
     - 🕥 In progress
   * - 0.19.1
     - ✅ Tested
   * - 0.20.0
     - ❌ Not tested

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

Installation
============

.. code-block:: console

   pip install dbt-coves

Main Features
=============

Project initialization
----------------------

.. code-block:: console

   dbt-coves init

Initializes a new ready-to-use dbt project that includes recommended integrations such as `sqlfluff <https://github.com/sqlfluff/sqlfluff>`_, `pre-commit <https://pre-commit.com/>`_, dbt packages, among others.

Uses a `cookiecutter <https://github.com/datacoves/cookiecutter-dbt>`_ template to make it easier to maintain.

Models generation
-----------------

.. code-block:: console

   dbt-coves generate <resource>

Where `<resource>` could be `sources`.

Code generation tool to easily generate models and model properties based on configuration and existing data.

Supports `Jinja <https://jinja.palletsprojects.com/>`_ templates to adjust how the resources are generated.

Quality Assurance
-----------------

.. code-block:: console

   dbt-coves check

Runs a set of checks in your local environment to ensure high code quality.

Checks can be extended by implementing `pre-commit hooks <https://pre-commit.com/#creating-new-hooks>`_.

CLI Reference
=============

.. argparse::
   :filename: dbt_coves/core/main.py
   :func: parser
   :prog: dbt_coves

Thanks
======

The project main structure was inspired by `dbt-sugar <https://github.com/bitpicky/dbt-sugar>`_. Special thanks to `Bastien Boutonnet <https://github.com/bastienboutonnet>`_ for the great work done.

Authors
=======

- Sebastian Sassi `@sebasuy <https://twitter.com/sebasuy>`_ – `Convexa <https://convexa.ai>`_
- Noel Gomez `@noel_g <https://twitter.com/noel_g>`_ – `Ninecoves <https://ninecoves.com>`_

About
=====

Learn more about `Datacoves <https://datacoves.com>`_.
