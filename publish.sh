#!/usr/bin/env bash

# Stop at the first failing step: a bump that doesn't happen used to carry on to
# `uv publish`, which PyPi then rejected for republishing the current version
set -euo pipefail

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

# bumpversion only knows about the files in .bumpversion.cfg, and the lock file
# records the project's own version too. Left behind, it comes back as an
# uncommitted change the next time anything runs `uv sync`, and bumpversion
# refuses to bump a dirty working directory.
uv lock
git add uv.lock
git diff --cached --quiet || git commit --amend --no-edit

git show --name-only
git push

uv build
uv publish
