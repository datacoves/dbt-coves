#!/usr/bin/env bash

./gen_readme.sh
poetry build
poetry publish
