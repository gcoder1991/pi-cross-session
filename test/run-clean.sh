#!/bin/bash
set -eu
cd "$(dirname "$0")/.."
NODE=${CROSS_TEST_NODE:-/Users/relvf/.nvm/versions/node/v22.23.1/bin/node}
NPM="$(dirname "$NODE")/../lib/node_modules/npm/bin/npm-cli.js"
if [ "${1:-}" = node ]; then
  shift
  exec /usr/bin/env -i PATH="$(dirname "$NODE"):/usr/bin:/bin:/usr/sbin:/sbin" "$NODE" --import ./test/support/clean-env.mjs "$@"
fi
exec /usr/bin/env -i PATH="$(dirname "$NODE"):/usr/bin:/bin:/usr/sbin:/sbin" "$NODE" --import ./test/support/clean-env.mjs "$NPM" "$@"
