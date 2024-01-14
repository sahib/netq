#!/bin/bash
set -euo pipefail

cloc --json $(find . -iname '*.go' ! -iname '*_test.go') | jq '.Go.code' | xargs -n1 printf "Actual code:\t%d Lines\n"
cloc --json $(find . -iname '*_test.go') | jq '.Go.code' | xargs -n1 printf "Test cases:\t%d Lines\n"
cloc --json $(find . -iname '*.go') | jq '.Go.code' | xargs -n1 printf "Total code:\t%d Lines\n"
