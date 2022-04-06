#!/usr/bin/env bash

# Run `poetry run towncrier create 123.feature` to update changelog

# poetry run towncrier build

# search and replace <current version> by <new version>, i.e. 1.0.4-a.3 by 1.0.4-a.4
# bumpversion <new_version>
poetry build
poetry publish
