#!/usr/bin/env sh
set -eu
repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -x "$repository_root/.tools/dotnet/dotnet" ]; then
    exec "$repository_root/.tools/dotnet/dotnet" "$@"
fi
exec dotnet "$@"
