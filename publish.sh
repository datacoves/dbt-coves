#!/usr/bin/env bash

# Run `poetry run towncrier create 123.feature` to update changelog
./gen_readme.sh
poetry run towncrier build
poetry build
poetry publish
