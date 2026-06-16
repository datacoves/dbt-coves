#!/usr/bin/env bash

# Run `uv run towncrier create 123.feature` to update changelog

# uv run towncrier build
die() {
    echo >&2 "$@"
    exit 1
}

if [ "$#" -eq 0 ]; then
    TYPE='patch'
elif
    [ $1 = 'major' ] ||
        [ $1 = 'minor' ] ||
        [ $1 = 'patch' ]
then
    TYPE=$1
else
    die "version type required: (major, minor, patch), $1 provided"

fi

uv run bumpversion $TYPE
git show --name-only
git push

uv build
uv publish
