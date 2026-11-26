#!/bin/sh
# Writes .env with freshly generated demo tokens (and their SHA-256 hashes) for docker compose.
# The plaintext tokens are stored in .env (git-ignored) so the README commands can use them.
set -eu
[ -e .env ] && { echo ".env already exists; remove it first" >&2; exit 1; }
token() { openssl rand -hex 24; }
hash() { printf %s "$1" | shasum -a 256 | cut -d' ' -f1; }
ingest=$(token); analyst=$(token); admin=$(token)
cat > .env <<ENV
FRAUDSTREAM_AUTH_TOKENS=ingestor:INGEST:$(hash "$ingest"),analyst-1:ANALYST:$(hash "$analyst"),operator:ADMIN:$(hash "$admin")
FRAUDSTREAM_INGEST_TOKEN=$ingest
FRAUDSTREAM_ANALYST_TOKEN=$analyst
FRAUDSTREAM_ADMIN_TOKEN=$admin
GRAFANA_ADMIN_PASSWORD=$(token)
ENV
chmod 600 .env
echo "wrote .env"
