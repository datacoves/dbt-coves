#!/usr/bin/env bash

rm dist/*
uv build --sdist

printf '\nRun "pip install -e ." in your environment.\n\n'
