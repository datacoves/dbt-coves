#!/usr/bin/env bash

docker build -t datacoves/dbt-coves .
docker push datacoves/dbt-coves

echo "You can now 'run docker run -it -v $PWD:/dbt -v /path/to/your/profiles.yml:/root/.dbt/profiles.yml datacoves/dbt-coves dbt-coves check'"
