#!/usr/bin/env bash
# Runs a command. Under github-oidc, it authenticates with file-oidc instead, with GitHub's ID token kept in a file.
# github-oidc exchanges a new token for each request, while file-oidc caches the exchanged token.
set -euo pipefail

if [[ "${DATABRICKS_AUTH_TYPE:-}" != "github-oidc" ]]; then
  exec "$@"
fi

: "${DATABRICKS_TOKEN_AUDIENCE:?DATABRICKS_TOKEN_AUDIENCE must be set for github-oidc}"
token_file="$RUNNER_TEMP/github-id-token"

fetch_token() {
  local token
  token=$(curl -sSf -G \
    -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
    --data-urlencode "audience=$DATABRICKS_TOKEN_AUDIENCE" \
    "$ACTIONS_ID_TOKEN_REQUEST_URL" | jq -r .value) || return 1
  [[ -n "$token" && "$token" != "null" ]] || return 1
  echo "::add-mask::$token"
  printf '%s' "$token" > "$token_file.tmp"
  mv "$token_file.tmp" "$token_file"
}

fetch_token
# GitHub's ID tokens are short-lived, and each new client exchanges the one in the file.
(while sleep 60; do fetch_token || echo "Failed to refresh GitHub's ID token" >&2; done) &
refresher=$!
trap 'kill "$refresher"' EXIT

export DATABRICKS_AUTH_TYPE=file-oidc
export DATABRICKS_OIDC_TOKEN_FILEPATH="$token_file"
"$@"
