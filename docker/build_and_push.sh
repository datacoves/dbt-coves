#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "No dbt version supplied, i.e. 0.21.0"
    exit 1
fi

if [ -z "$2" ]
  then
    echo "No dbt-coves version supplied, i.e. 0.21.0a1"
    exit 1
fi

docker build -t datacoves/dbt-coves:latest --build-arg DBT_VERSION="$1" --build-arg DBT_COVES_VERSION="$2" .
docker build -t datacoves/dbt-coves:"$1"-"$2" --build-arg DBT_VERSION="$1" --build-arg DBT_COVES_VERSION="$2" .
docker push datacoves/dbt-coves:latest
docker push datacoves/dbt-coves:"$1"-"$2"

echo "You can now 'run docker run -it -v $PWD:/usr/app -v /path/to/your/profiles.yml:/root/.dbt/profiles.yml datacoves/dbt-coves check'"
