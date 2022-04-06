#!/usr/bin/env bash

# Run `poetry run towncrier create 123.feature` to update changelog

# poetry run towncrier build

#major
#minor
#patch
#release
#build

TYPE=$1

if [ $TYPE = 'major' ] || [ $TYPE = 'minor' ] || [ $TYPE = 'patch' ] || [ $TYPE = 'release' ] || [ $TYPE = 'build' ]
then
    echo "Bumping $TYPE version"
    bumpversion $TYPE
else
    echo "$TYPE invalid version type"
fi

# search and replace <current version> by <new version>, i.e. 1.0.4-a.3 by 1.0.4-a.4

# bumpversion <new_version>
#poetry build
#poetry publish