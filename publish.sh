#!/usr/bin/env bash

./gen_readme.sh
poetry run towncrier build
poetry build
poetry publish
