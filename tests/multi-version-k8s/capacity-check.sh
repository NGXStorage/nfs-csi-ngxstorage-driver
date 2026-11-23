#!/usr/bin/env bash
set -euo pipefail

# Check backend share capacity via NGX storage API.
# Usage: STORAGE_API_KEY=... STORAGE_API_URL=https://controller $0 <volumeHandle>
key="${STORAGE_API_KEY:?STORAGE_API_KEY must be supplied in the environment}"
volume_id="${1:?usage: capacity-check.sh <volumeHandle>}"
api_url="${STORAGE_API_URL:?STORAGE_API_URL must be supplied in the environment (no lab default is baked into this script)}"

curl -sk --config <(printf 'header = "Authorization: Bearer %s"\n' "$key") \
  "$api_url/api/v2/share/$volume_id" 2>/dev/null | python3 -c '
import json
import sys

try:
    data = json.load(sys.stdin)
    capacity = data.get("capacity", {})
    print("share={} pool={} used={}B soft_quota={}B data={}B".format(
        data.get("name"), data.get("pool_name"), capacity.get("used"),
        capacity.get("soft_quota"), capacity.get("data")))
except Exception as error:
    print("parse-error: {}".format(error))
'
