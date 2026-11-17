#!/usr/bin/env bash
# The repo's one check: formatting, vet and the full test suite. CI and the
# pre-commit hook both run this script, so they always agree.
set -euo pipefail
cd "$(dirname "$0")/.."

# the pre-commit hook runs this in a copy of the staged files, outside git: every Go file there is staged
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	unformatted=$(git ls-files -z '*.go' | xargs -0 gofmt -l)
else
	unformatted=$(find . -name '*.go' -print0 | xargs -0 gofmt -l)
fi
if [[ -n "$unformatted" ]]; then
	echo "not gofmt-formatted (run gofmt -w on these):" >&2
	echo "$unformatted" >&2
	exit 1
fi
go vet ./...
go test ./...
