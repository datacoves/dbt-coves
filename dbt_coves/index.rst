
Installation
============

.. code-block:: console

   pip install dbt-coves

We recommend using `python virtualenvs <https://docs.python.org/3/tutorial/venv.html>`_ and create one separate environment per project.

⚠️ **if you have dbt < 0.18.0 installed, dbt-coves will automatically upgrade dbt to the latest version**

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

Settings
========

Dbt-coves could optionally read settings from ``.dbt_coves.yml``. A standard settings files could looke like this:

.. code-block:: yaml

   generate:
     sources:
       schemas:
         - RAW
       destination: "models/sources/{{ schema }}/{{ relation }}.sql"
       model_props_strategy: one_file_per_model
       templates_folder: "templates"

In this example options for the ``generate`` command are provided:

``schemas``: List of schema names where to look for source tables

``destination``: Path to generated model, where ``schema`` represents the lowercased schema and ``relation`` the lowercased table name.

``model_props_strategy``: Defines how dbt-coves generates model properties files, currently just ``one_file_per_model`` is available, creates one yaml file per model.

``templates_folder``: Folder where source generation jinja templates are located.

Override source generation templates
------------------------------------

Customizing generated models and model properties requires placing specific files under the ``templates_folder`` folder like these:

source_model.sql
~~~~~~~~~~~~~~~~

.. code-block:: sql

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
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

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
======================

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
