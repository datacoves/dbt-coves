
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
   * - 0.17.0
     - ❌ Not supported
   * - 0.18.x
     - ✅ Tested
   * - 0.19.x
     - ✅ Tested
   * - 0.20.x
     - ✅ Tested
   * - 0.21.x
     - 🕥 In progress

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
************

.. code:: console

   pip install dbt-coves

We recommend using `python virtualenvs
<https://docs.python.org/3/tutorial/venv.html>`_ and create one
separate environment per project.

⚠️ **if you have dbt < 0.18.0 installed, dbt-coves will automatically
upgrade dbt to the latest version**


Main Features
*************


Project initialization
======================

.. code:: console

   dbt-coves init

Initializes a new ready-to-use dbt project that includes recommended
integrations such as `sqlfluff
<https://github.com/sqlfluff/sqlfluff>`_, `pre-commit
<https://pre-commit.com/>`_, dbt packages, among others.

Uses a `cookiecutter <https://github.com/datacoves/cookiecutter-dbt>`_
template to make it easier to maintain.


Models generation
=================

.. code:: console

   dbt-coves generate <resource>

Where *<resource>* could be *sources*.

Code generation tool to easily generate models and model properties
based on configuration and existing data.

Supports `Jinja <https://jinja.palletsprojects.com/>`_ templates to
adjust how the resources are generated.


Quality Assurance
=================

.. code:: console

   dbt-coves check

Runs a set of checks in your local environment to ensure high code
quality.

Checks can be extended by implementing `pre-commit hooks
<https://pre-commit.com/#creating-new-hooks>`_.


Settings
********

Dbt-coves could optionally read settings from ``.dbt_coves.yml``. A
standard settings files could looke like this:

.. code:: yaml

   generate:
     sources:
       schemas:
         - RAW
       destination: "models/sources/{{ schema }}/{{ relation }}.sql"
       model_props_strategy: one_file_per_model
       templates_folder: "templates"

In this example options for the ``generate`` command are provided:

``schemas``: List of schema names where to look for source tables

``destination``: Path to generated model, where ``schema`` represents
the lowercased schema and ``relation`` the lowercased table name.

``model_props_strategy``: Defines how dbt-coves generates model
properties files, currently just ``one_file_per_model`` is available,
creates one yaml file per model.

``templates_folder``: Folder where source generation jinja templates
are located.


Override source generation templates
====================================

Customizing generated models and model properties requires placing
specific files under the ``templates_folder`` folder like these:


source_model.sql
----------------

.. code:: sql

   with raw_source as (

       select * from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

   ),

   final as (

       select
   {%- for col in columns %}
           {{ col.name.lower() }}{% if not loop.last or nested %},{% endif %}
   {%- endfor %}
   {%- if adapter_name == 'SnowflakeAdapter' %}
   {%- for key, cols in nested.items() %}
     {%- for col in cols %}
           {{ key }}:{{ col.lower() }}::varchar as {{ col.lower() }}{% if not loop.last %},{% endif %}
     {%- endfor %}
   {%- endfor %}
   {%- elif adapter_name == 'BigQueryAdapter' %}
   {%- for key, cols in nested.items() %}
     {%- for col in cols %}
           cast({{ key }}.{{ col.lower() }} as string) as {{ col.lower() }}{% if not loop.last %},{% endif %}
     {%- endfor %}
   {%- endfor %}
   {%- elif adapter_name == 'RedshiftAdapter' %}
   {%- for key, cols in nested.items() %}
     {%- for col in cols %}
           {{ key }}.{{ col.lower() }}::varchar as {{ col.lower() }}{% if not loop.last %},{% endif %}
     {%- endfor %}
   {%- endfor %}
   {%- endif %}

       from raw_source

   )

   select * from final


source_model_props.yml
----------------------

.. code:: yaml

   version: 2

   sources:
     - name: {{ relation.schema.lower() }}
       schema: {{ relation.schema.lower() }}
       tables:
         - name: {{ relation.name.lower() }}
           identifier: {{ relation.name }}

   models:
     - name: {{ model.lower() }}
       columns:
   {%- for col in columns %}
         - name: {{ col.name.lower() }}
   {%- endfor %}
   {%- for cols in nested.values() %}
     {%- for col in cols %}
         - name: {{ col }}
     {%- endfor %}
   {%- endfor %}


CLI Detailed Reference
**********************

CLI tool for dbt users applying analytics engineering best practices.

::

   usage: dbt_coves [-h] [-v] {init,generate,check,fix} ...


Named Arguments
===============

-v, --version

show program’s version number and exit


dbt-coves commands
==================

task

Possible choices: init, generate, check, fix


Sub-commands:
=============


init
----

Initializes a new dbt project using predefined conventions.

::

   dbt_coves init [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] [--template TEMPLATE] [--current-dir]


Named Arguments
~~~~~~~~~~~~~~~

--log-level

overrides default log level

Default: “”

-vv, --verbose

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path

Full path to .dbt_coves.yml file if not using default. Default is
current working directory.

--project-dir

Which directory to look in for the dbt_project.yml file. Default is
the current working directory and its parents.

--profiles-dir

Which directory to look in for the profiles.yml file.

Default: “~/.dbt”

--profile

Which profile to load. Overrides setting in dbt_project.yml.

-t, --target

Which target to load for the given profile

--vars

Supply variables to your dbt_project.yml file. This argument should be
a YAML string, eg. ‘{my_variable: my_value}’

Default: “{}”

--template

Cookiecutter template github url, i.e.
‘https://github.com/datacoves/cookiecutter-dbt-coves.git’

--current-dir

Generate the dbt project in the current directory.

Default: False


generate
--------

Generates sources and models with defaults.

::

   dbt_coves generate [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] {sources} ...


Named Arguments
~~~~~~~~~~~~~~~

--log-level

overrides default log level

Default: “”

-vv, --verbose

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path

Full path to .dbt_coves.yml file if not using default. Default is
current working directory.

--project-dir

Which directory to look in for the dbt_project.yml file. Default is
the current working directory and its parents.

--profiles-dir

Which directory to look in for the profiles.yml file.

Default: “~/.dbt”

--profile

Which profile to load. Overrides setting in dbt_project.yml.

-t, --target

Which target to load for the given profile

--vars

Supply variables to your dbt_project.yml file. This argument should be
a YAML string, eg. ‘{my_variable: my_value}’

Default: “{}”


dbt-coves generate commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~

task

Possible choices: sources


Sub-commands:
~~~~~~~~~~~~~


sources
"""""""

Generate source dbt models by inspecting the database schemas and
relations.

::

   dbt_coves generate sources [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] [--schemas SCHEMAS]
                              [--relations RELATIONS] [--destination DESTINATION] [--model_props_strategy MODEL_PROPS_STRATEGY] [--templates_folder TEMPLATES_FOLDER]


Named Arguments
+++++++++++++++

--log-level

overrides default log level

Default: “”

-vv, --verbose

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path

Full path to .dbt_coves.yml file if not using default. Default is
current working directory.

--project-dir

Which directory to look in for the dbt_project.yml file. Default is
the current working directory and its parents.

--profiles-dir

Which directory to look in for the profiles.yml file.

Default: “~/.dbt”

--profile

Which profile to load. Overrides setting in dbt_project.yml.

-t, --target

Which target to load for the given profile

--vars

Supply variables to your dbt_project.yml file. This argument should be
a YAML string, eg. ‘{my_variable: my_value}’

Default: “{}”

--schemas

Comma separated list of schemas where raw data resides, i.e.
‘RAW_SALESFORCE,RAW_HUBSPOT’

--relations

Comma separated list of relations where raw data resides, i.e.
‘RAW_HUBSPOT_PRODUCTS,RAW_SALESFORCE_USERS’

--destination

Where models sql files will be generated, i.e.
‘models/{schema_name}/{relation_name}.sql’

--model_props_strategy

Strategy for model properties files generation, i.e.
‘one_file_per_model’

--templates_folder

Folder with jinja templates that override default sources generation
templates, i.e. ‘templates’


check
-----

Runs pre-commit hooks and linters.

::

   dbt_coves check [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] [--no-fix]


Named Arguments
~~~~~~~~~~~~~~~

--log-level

overrides default log level

Default: “”

-vv, --verbose

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path

Full path to .dbt_coves.yml file if not using default. Default is
current working directory.

--project-dir

Which directory to look in for the dbt_project.yml file. Default is
the current working directory and its parents.

--profiles-dir

Which directory to look in for the profiles.yml file.

Default: “~/.dbt”

--profile

Which profile to load. Overrides setting in dbt_project.yml.

-t, --target

Which target to load for the given profile

--vars

Supply variables to your dbt_project.yml file. This argument should be
a YAML string, eg. ‘{my_variable: my_value}’

Default: “{}”

--no-fix

Do not suggest auto-fixing linting errors. Useful when running this
command on CI jobs.

Default: False


fix
---

Runs linter fixes.

::

   dbt_coves fix [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS]


Named Arguments
~~~~~~~~~~~~~~~

--log-level

overrides default log level

Default: “”

-vv, --verbose

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path

Full path to .dbt_coves.yml file if not using default. Default is
current working directory.

--project-dir

Which directory to look in for the dbt_project.yml file. Default is
the current working directory and its parents.

--profiles-dir

Which directory to look in for the profiles.yml file.

Default: “~/.dbt”

--profile

Which profile to load. Overrides setting in dbt_project.yml.

-t, --target

Which target to load for the given profile

--vars

Supply variables to your dbt_project.yml file. This argument should be
a YAML string, eg. ‘{my_variable: my_value}’

Default: “{}”

Select one of the available sub-commands with –help to find out more
about them.


Thanks
******

The project main structure was inspired by `dbt-sugar
<https://github.com/bitpicky/dbt-sugar>`_. Special thanks to `Bastien
Boutonnet <https://github.com/bastienboutonnet>`_ for the great work
done.


Authors
*******

*  Sebastian Sassi `@sebasuy <https://twitter.com/sebasuy>`_ –
   `Convexa <https://convexa.ai>`_

*  Noel Gomez `@noel_g <https://twitter.com/noel_g>`_ – `Ninecoves
   <https://ninecoves.com>`_


About
*****

Learn more about `Datacoves <https://datacoves.com>`_.
