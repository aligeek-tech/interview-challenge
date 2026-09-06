#!/usr/bin/env sh
set -eu
repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"
: "${TEST_DATABASE_URL:?Set TEST_DATABASE_URL to an isolated PostgreSQL server role with CREATEDB permission.}"
./scripts/dotnet.sh restore CapacityBooking.sln --locked-mode
./scripts/dotnet.sh format CapacityBooking.sln --verify-no-changes --no-restore
./scripts/dotnet.sh build CapacityBooking.sln -c Release --no-restore
./scripts/dotnet.sh test CapacityBooking.sln -c Release --no-build --no-restore \
    --logger trx --results-directory artifacts/test-results "$@"
