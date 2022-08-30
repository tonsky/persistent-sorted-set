#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "$(dirname "$0")/.."

clj -M:bench -m me.tonsky.persistent-sorted-set.bench