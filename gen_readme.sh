#!/usr/bin/env bash

rm -rf docs/build

poetry run sphinx-build -c docs -b rst dbt_coves docs/build

cp docs/header.rst README.rst
cat docs/build/index.rst >> README.rst

sed -i '' -e "s/\/.*\/\.dbt/\~\/\.dbt/g" README.rst
