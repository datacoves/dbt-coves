## Extract configuration from Airbyte

```console
dbt-coves extract airbyte
```

Extracts the configuration from your Airbyte sources, connections and destinations (excluding credentials) and stores it in the specified folder. The main goal of this feature is to keep track of the configuration changes in your git repo, and rollback to a specific version when needed.

Full usage example:

```console
dbt-coves extract airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load/airbyte
```

## Load configuration to Airbyte

```console
dbt-coves load airbyte
```

Loads the Airbyte configuration generated with `dbt-coves extract airbyte` on an Airbyte server. Secrets folder needs to be specified separately. You can use [git-secret](https://git-secret.io/) to encrypt secrets and make them part of your git repo.

### Loading secrets

Secret credentials can be approached in two different ways: locally or remotely (through a provider/manager).

In order to load encrypted fields locally:

```console
dbt-coves load airbyte --secrets-path /path/to/secret/directory

# This directory must have 'sources', 'destinations' and 'connections' folders nested inside, and inside them the respective JSON files with unencrypted fields.
# Naming convention: JSON unencrypted secret files must be named exactly as the extracted ones.
```

To load encrypted fields through a manager (in this case we are connecting to Datacoves' Service Credentials):

```console
--secrets-manager datacoves
```

```console
--secrets-url https://api.datacoves.localhost/service-credentials/airbyte
```

```console
--secrets-token <secret token>
```

Full usage example:

```console
dbt-coves load airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load/airbyte --secrets-path /config/workspace/secrets
```
