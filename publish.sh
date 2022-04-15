#!/usr/bin/env bash

# Run `poetry run towncrier create 123.feature` to update changelog

# poetry run towncrier build
die() {
    echo >&2 "$@"
    exit 1
}

if [ "$#" -eq 0 ]; then
    TYPE='build'
elif
    [ $1 = 'major' ] ||
        [ $1 = 'minor' ] ||
        [ $1 = 'patch' ] ||
        [ $1 = 'release' ] ||
        [ $1 = 'build' ]
then
    TYPE=$1
else
    die "version type required: (major, minor, patch, release, build), $1 provided"

fi

poetry run bumpversion $TYPE
git show --name-only
git push

poetry build
poetry publish
