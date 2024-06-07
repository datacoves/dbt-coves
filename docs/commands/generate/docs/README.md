## dbt-coves generate docs

You can use dbt-coves to improve the standard dbt docs generation process. It generates your dbt docs, updates external links so they always open in a new tab. It also has the option to merge production `catalog.json` into the local environment when running in deferred mode, so you can run [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) hooks even when the model has not been run locally.

### Arguments

`dbt-coves generate docs` supports the following args:

```console
--merge-deferred
# Merge a deferred catalog.json into your generated one.
# Flag: no value required.
```

```
--state
# Directory where your production catalog.json is located
# Mandatory when using --merge-deferred
```
