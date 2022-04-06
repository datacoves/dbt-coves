#!/usr/bin/env bash

# Run `poetry run towncrier create 123.feature` to update changelog

# poetry run towncrier build
die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument (version type) required, $# provided"
[ $1 = 'major' ] || 
[ $1 = 'minor' ] || 
[ $1 = 'patch' ] || 
[ $1 = 'release' ] || 
[ $1 = 'build' ] || 
die "version type required: (major, minor, patch, release, build), $1 provided"


TYPE=$1
# bumpversion -> search and replace versions, i.e. 1.0.4-a.3 by 1.0.4-a.4
bumpversion $TYPE
poetry build
poetry publish

