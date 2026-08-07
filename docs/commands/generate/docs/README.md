## dbt-coves generate docs

You can use dbt-coves to improve the standard dbt docs generation process. It generates your dbt docs, updates external links so they always open in a new tab. It also has the option to merge production `catalog.json` into the local environment when running in deferred mode, so you can run [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) hooks even when the model has not been run locally such as when using Slim CI.

### Arguments

`dbt-coves generate docs` supports the following args:

```console
--merge-deferred
# Merge a deferred catalog.json into your generated one.
# Flag: no value required.
```

```console
--state
# Directory where your production catalog.json is located
# Mandatory when using --merge-deferred
```

```console
--dbt-args
# A single, double-quoted string of extra args to forward to the underlying `dbt docs generate`
# call, e.g. "--no-compile --select foo --exclude bar"
```

### Discussion

- This command always runs `dbt docs generate` first (any `--dbt-args` are appended to that call), then post-processes the resulting `target/index.html` so external links open in a new tab instead of replacing the docs frame.
- With `--merge-deferred`, dbt-coves reads `catalog.json` from both `target/` (local) and `--state` (the deferred/production one), and copies over any node or source present in the deferred catalog but missing locally - so tools like [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) that inspect `catalog.json` see the full project even when only a subset of models were actually built locally (e.g. Slim CI runs that only build modified models).
- `--state` is required whenever `--merge-deferred` is passed, and must point at a directory that already contains a `catalog.json` (i.e. `dbt docs generate` - or this command - must have already run there); otherwise the command raises before attempting the merge.

### Sample usage

```console
# Plain docs generation with fixed external links
dbt-coves generate docs

# Slim CI: merge in the deferred production catalog so dbt-checkpoint sees the full project
dbt-coves generate docs --merge-deferred --state logs/

# Forward extra flags straight to `dbt docs generate`
dbt-coves generate docs --dbt-args "--no-compile --select foo --exclude bar"
```
