#!/usr/bin/env bash

docker build -t datacoves/dbt-coves:latest .
docker build -t datacoves/dbt-coves:0.20.0-a.3 .
docker push datacoves/dbt-coves:latest
docker push datacoves/dbt-coves:0.20.0-a.3

echo "You can now 'run docker run -it -v $PWD:/usr/app -v /path/to/your/profiles.yml:/root/.dbt/profiles.yml datacoves/dbt-coves check'"
