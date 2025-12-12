#!/bin/bash
branch_name=$(git symbolic-ref --short HEAD)

if [ "$branch_name" = "main" ]; then
  echo "🚫 Pushes to main are not allowed. Please push to a feature or release branch."
  exit 1
fi

exit 0
