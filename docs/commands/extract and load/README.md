# Extract and load

## Overview

`dbt-coves extract <provider>` and `dbt-coves load <provider>` save and restore the configuration (sources, connections, destinations - excluding credentials) of a data-replication provider, so changes can be tracked in git and rolled back.

```console
dbt-coves extract <provider>
dbt-coves load <provider>
```

Where `provider` is one of:

- [_airbyte_](airbyte/): extract/load an Airbyte instance's configuration.
- [_fivetran_](fivetran/): extract/load a Fivetran instance's configuration.
