CLI tool for dbt users applying analytics engineering best practices.

::

   usage: dbt_coves [-h] [-v] {init,generate,check,fix} ...


Named Arguments
***************

-v, --version     

show program’s version number and exit


dbt-coves commands
******************

task     

Possible choices: init, generate, check, fix


Sub-commands:
*************


init
====

Initializes a new dbt project using predefined conventions.

::

   dbt_coves init [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] [--template TEMPLATE]


Named Arguments
---------------

--log-level     

overrides default log level

Default: “”

-vv, --verbose     

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path     

Full path to .dbt_coves file if not using default.

--project-dir     ..

   Which directory to look in for the dbt_project.yml file.
   Default is the current working directory and its parents.

--profiles-dir     ..

   Which directory to look in for the profiles.yml file. Default = /Users/ssassi/.dbt

Default: “/Users/ssassi/.dbt”

--profile     ..

   Which profile to load. Overrides setting in dbt_project.yml.

-t, --target     ..

   Which target to load for the given profile

--vars     ..

   Supply variables to the project. This argument overrides variables
   defined in your dbt_project.yml file. This argument should be a YAML
   string, eg. ‘{my_variable: my_value}’

Default: “{}”

--template     ..

   Cookiecutter template github url, i.e. ‘`https://github.com/datacoves/cookiecutter-dbt-coves.git <https://github.com/datacoves/cookiecutter-dbt-coves.git>`_’


generate
========

Generates sources and models with defaults.

::

   dbt_coves generate [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] {sources} ...


Named Arguments
---------------

--log-level     

overrides default log level

Default: “”

-vv, --verbose     

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path     

Full path to .dbt_coves file if not using default.

--project-dir     ..

   Which directory to look in for the dbt_project.yml file.
   Default is the current working directory and its parents.

--profiles-dir     ..

   Which directory to look in for the profiles.yml file. Default = /Users/ssassi/.dbt

Default: “/Users/ssassi/.dbt”

--profile     ..

   Which profile to load. Overrides setting in dbt_project.yml.

-t, --target     ..

   Which target to load for the given profile

--vars     ..

   Supply variables to the project. This argument overrides variables
   defined in your dbt_project.yml file. This argument should be a YAML
   string, eg. ‘{my_variable: my_value}’

Default: “{}”


dbt-coves generate commands
---------------------------

task     

Possible choices: sources


Sub-commands:
-------------


sources
~~~~~~~

Generate source dbt models by inspecting the database schemas and relations.

::

   dbt_coves generate sources [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] [--schemas SCHEMAS]
                              [--destination DESTINATION] [--model_props_strategy MODEL_PROPS_STRATEGY] [--templates_folder TEMPLATES_FOLDER]


Named Arguments
"""""""""""""""

--log-level     

overrides default log level

Default: “”

-vv, --verbose     

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path     

Full path to .dbt_coves file if not using default.

--project-dir     ..

   Which directory to look in for the dbt_project.yml file.
   Default is the current working directory and its parents.

--profiles-dir     ..

   Which directory to look in for the profiles.yml file. Default = /Users/ssassi/.dbt

Default: “/Users/ssassi/.dbt”

--profile     ..

   Which profile to load. Overrides setting in dbt_project.yml.

-t, --target     ..

   Which target to load for the given profile

--vars     ..

   Supply variables to the project. This argument overrides variables
   defined in your dbt_project.yml file. This argument should be a YAML
   string, eg. ‘{my_variable: my_value}’

Default: “{}”

--schemas     ..

   Comma separated list of schemas where raw data resides, i.e. ‘RAW_SALESFORCE,RAW_HUBSPOT’

--destination     ..

   Where models sql files will be generated, i.e. ‘models/{schema_name}/{relation_name}.sql’

--model_props_strategy     ..

   Strategy for model properties files generation, i.e. ‘one_file_per_model’

--templates_folder     ..

   Folder with jinja templates that override default sources generation templates, i.e. ‘templates’


check
=====

Runs pre-commit hooks and linters.

::

   dbt_coves check [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS] [--no-fix]


Named Arguments
---------------

--log-level     

overrides default log level

Default: “”

-vv, --verbose     

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path     

Full path to .dbt_coves file if not using default.

--project-dir     ..

   Which directory to look in for the dbt_project.yml file.
   Default is the current working directory and its parents.

--profiles-dir     ..

   Which directory to look in for the profiles.yml file. Default = /Users/ssassi/.dbt

Default: “/Users/ssassi/.dbt”

--profile     ..

   Which profile to load. Overrides setting in dbt_project.yml.

-t, --target     ..

   Which target to load for the given profile

--vars     ..

   Supply variables to the project. This argument overrides variables
   defined in your dbt_project.yml file. This argument should be a YAML
   string, eg. ‘{my_variable: my_value}’

Default: “{}”

--no-fix     ..

   Do not suggest auto-fixing linting errors. Useful when running this command on CI jobs.

Default: False


fix
===

Runs linter fixes.

::

   dbt_coves fix [-h] [--log-level LOG_LEVEL] [-vv] [--config-path CONFIG_PATH] [--project-dir PROJECT_DIR] [--profiles-dir PROFILES_DIR] [--profile PROFILE] [-t TARGET] [--vars VARS]


Named Arguments
---------------

--log-level     

overrides default log level

Default: “”

-vv, --verbose     

When provided the length of the tracebacks will not be truncated.

Default: False

--config-path     

Full path to .dbt_coves file if not using default.

--project-dir     ..

   Which directory to look in for the dbt_project.yml file.
   Default is the current working directory and its parents.

--profiles-dir     ..

   Which directory to look in for the profiles.yml file. Default = /Users/ssassi/.dbt

Default: “/Users/ssassi/.dbt”

--profile     ..

   Which profile to load. Overrides setting in dbt_project.yml.

-t, --target     ..

   Which target to load for the given profile

--vars     ..

   Supply variables to the project. This argument overrides variables
   defined in your dbt_project.yml file. This argument should be a YAML
   string, eg. ‘{my_variable: my_value}’

Default: “{}”

Select one of the available sub-commands with –help to find out more about them.
