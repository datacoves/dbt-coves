
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

Metadata
~~~~~~~~

Supports the argument `--metadata` which allows to specify a csv file containing field types and descriptions to be inserted into the model property files.

.. code-block:: console

   dbt-coves generate sources --metadata metadata.csv


Metadata format:

.. list-table::
   :header-rows: 1

   * - database
     - schema
     - relation
     - column
     - key
     - type
     - description
   * - raw
     - master
     - person
     - name
     - (empty)
     - varchar
     - The full name of the person
   * - raw
     - master
     - person
     - name
     - groupName
     - varchar
     - The group name of the person

Quality Assurance
-----------------

.. code-block:: console

   dbt-coves check

Runs a set of checks in your local environment to ensure high code quality.

Checks can be extended by implementing `pre-commit hooks <https://pre-commit.com/#creating-new-hooks>`_.

Environment setup
-----------------

.. code-block:: console

   dbt-coves setup

Runs a set of checks in your local environment and helps you configure it properly: ssh key, git, dbt profiles.yml, vscode extensions.

Extract configuration from Airbyte
----------------------------------

.. code-block:: console

   dbt-coves extract airbyte

Extracts the configuration from your Airbyte sources, connections and destinations (excluding credentials) and stores it in the specified folder. The main goal of this feature is to keep track of the configuration changes in your git repo, and rollback to a specific version when needed.

Load configuration to Airbyte
-----------------------------

.. code-block:: console

   dbt-coves load airbyte

Loads the Airbyte configuration generated with `dbt-coves extract airbyte` on an Airbyte server. Secrets folder needs to be specified separatedly. You can use `git-secret <https://git-secret.io/>`_ to encrypt them and make them part of your git repo.

Settings
========

Dbt-coves could optionally read settings from ``.dbt_coves.yml`` or ``.dbt_coves/config.yml``. A standard settings files could looke like this:

.. code-block:: yaml

  generate:
    sources:
      schemas:
        - RAW
      destination: "models/sources/{{ schema }}/{{ relation }}.sql"
      model_props_strategy: one_file_per_model
      templates_folder: ".dbt_coves/templates"


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

        select
            *
        from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

    ),

    final as (

        select
    {%- if adapter_name == 'SnowflakeAdapter' %}
    {%- for key, cols in nested.items() %}
      {%- for col in cols %}
            {{ key }}:{{ '"' + col + '"' }}::{{ cols[col]["type"] }} as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
      {%- endfor %}
    {%- endfor %}
    {%- elif adapter_name == 'BigQueryAdapter' %}
    {%- for key, cols in nested.items() %}
      {%- for col in cols %}
            cast({{ key }}.{{ col }} as {{ cols[col]["type"].replace("varchar", "string") }}) as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
      {%- endfor %}
    {%- endfor %}
    {%- elif adapter_name == 'RedshiftAdapter' %}
    {%- for key, cols in nested.items() %}
      {%- for col in cols %}
            {{ key }}.{{ col }}::{{ cols[col]["type"] }} as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
      {%- endfor %}
    {%- endfor %}
    {%- endif %}
    {%- for col in columns %}
            {{ '"' + col.name + '"' }} as {{ col.name.lower() }}{% if not loop.last %},{% endif %}
    {%- endfor %}

        from raw_source

    )

    select * from final


source_model_props.yml
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    version: 2

    sources:
      - name: {{ relation.schema.lower() }}
    {%- if source_database %}
        database: {{ source_database }}
    {%- endif %}
        schema: {{ relation.schema.lower() }}
        tables:
          - name: {{ relation.name.lower() }}
            identifier: {{ relation.name }}

    models:
      - name: {{ model.lower() }}
        columns:
    {%- for cols in nested.values() %}
      {%- for col in cols %}
          - name: {{ cols[col]["id"] }}
          {%- if cols[col]["description"] %}
            description: "{{ cols[col]['description'] }}"
          {%- endif %}
      {%- endfor %}
    {%- endfor %}
    {%- for col in columns %}
          - name: {{ col.name.lower() }}
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
