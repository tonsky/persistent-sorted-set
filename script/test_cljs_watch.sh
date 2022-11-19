#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "$(dirname "$0")/.."

yarn shadow-cljs watch test --config-merge '{:autorun true}'