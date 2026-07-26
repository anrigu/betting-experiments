#!/usr/bin/env bash
# Trigger Render deploys for both services after a push.
# Auto-deploy is unavailable until Render's GitHub App is installed on the
# `anrigu` account (Render dashboard -> Settings -> Git). Until then:
#   RENDER_API_KEY=... ./scripts/deploy.sh
set -euo pipefail
: "${RENDER_API_KEY:?set RENDER_API_KEY}"
for sid in srv-d9j1v87aqgkc73arjedg srv-d9j1v8btqb8s739ktf7g; do
  curl -sf -X POST -H "Authorization: Bearer $RENDER_API_KEY" \
    -H "Content-Type: application/json" \
    "https://api.render.com/v1/services/$sid/deploys" -d '{}' \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("id"), d.get("status"))'
done
