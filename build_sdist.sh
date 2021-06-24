#!/usr/bin/env bash

rm dist/*
poetry build --format sdist
tar -xvf dist/*.tar.gz -O '*/setup.py' > setup.py

printf '\nRun "pip install -e ." in your environment.\n\n'
