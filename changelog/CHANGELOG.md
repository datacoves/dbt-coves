## dbt-coves [0.20.0-a.2] - 2021-08-06
### Features


- [#16](https://github.com/datacoves/dbt-coves/issues/16) Select which schemas to inspect when generating sources, i.e. `dbt-coves generate sources --shemas=RAW_*`.

  Select which relations to inspect as well by running i.e. `dbt-coves generate sources --relations=S*RC_*`.

  Both `schemas` and `relations` selectors can be combined in the same run.


## dbt-coves [0.20.0-a.1] - 2021-07-28
### Bug Fixes


- [#5](https://github.com/datacoves/dbt-coves/issues/5) Generate source throws exception when VARIANT contains no json.
  


### Features


- [#24](https://github.com/datacoves/dbt-coves/issues/24) When initializing a new dbt project, it's good to create every file in the current folder instead of on a new one.
  By passing the argument --current-dir, the initialization will happen in the current directory.
