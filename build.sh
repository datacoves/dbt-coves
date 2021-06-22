#!/usr/bin/env bash

poetry build --format sdist
tar -xvf dist/*`poetry version -s`.tar.gz -O '*/setup.py' > setup.py

printf '\nRun "pip install -e ." in your environment.\n\n'
