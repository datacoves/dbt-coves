## Extract configuration from Fivetran

```console
dbt-coves extract fivetran
```

Extracts the configuration from your Fivetran destinations and connectors (excluding credentials) and stores it in the specified folder. The main goal of this feature is to keep track of the configuration changes in your git repo, and rollback to a specific version when needed.

Full usage example:

```console
dbt-coves extract fivetran --credentials /config/workspace/secrets/fivetran/credentials.yml --path /config/workspace/load/fivetran
```

## Load configuration to Fivetran

```console
dbt-coves load fivetran
```

Loads the Fivetran configuration generated with `dbt-coves extract fivetran` on a Fivetran instance. Secrets folder needs to be specified separately. You can use [git-secret](https://git-secret.io/) to encrypt secrets and make them part of your git repo.

### Credentials

In order for extract/load fivetran to work properly, you need to provide an api key-secret pair (you can generate them [here](https://fivetran.com/account/settings/account)).

These credentials can be used with `--api-key [key] --api-secret [secret]`, or specyfing a YML file with `--credentials /path/to/credentials.yml`. The required structure of this file is the following:

```yaml
account_name: # Any name, used by dbt-coves to ask which to use when more than one is found
  api_key: [key]
  api_secret: [secret]
account_name_2:
  api_key: [key]
  api_secret: [secret]
```

This YML file approach allows you to both work with multiple Fivetran accounts, and treat this credentials file as a secret.

> :warning: **Warning**: --api-key/secret and --credentials flags are mutually exclusive, don't use them together.

### Loading secrets

Secret credentials can be approached via `--secrets-path` flag

```console
dbt-coves load fivetran --secrets-path /path/to/secret/directory
```

#### Field naming convention

Although secret files can have any name, unencrypted JSON files must follow a simple structure:

- Keys should match their corresponding Fivetran destination ID: two words automatically generated by Fivetran, which can be found in previously extracted data.
- Inside those keys, a nested dictionary of which fields should be overwritten

For example:

```json
{
  "extract_muscle": {
    // Internal ID that Fivetran gave to a Snowflake warehouse Destination
    "password": "[PASSWORD]" // Field:Value pair
  },
  "centre_straighten": {
    "password": "[PASSWORD]"
  }
}
```
