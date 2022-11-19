#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "$(dirname "$0")/.."

yarn shadow-cljs release bench
node target/bench.js $@