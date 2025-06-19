#!/bin/bash

METABASE_HOST="http://metabase:3000"

echo "$(date "+%Y-%m-%d %T") [INFO] Metabase available. Starting config"

# 1º Getting setup-token
setup_token=$(curl -s "$METABASE_HOST/api/session/properties" | jq '."setup-token"')

if [ "$setup_token" = "null" ] || [ -z "$setup_token" ]; then
  echo "$(date "+%Y-%m-%d %T") [ERROR] Setup fail. Please check if Metabase change "setup-token" key name."
  exit 1
fi

echo "$(date "+%Y-%m-%d %T") [INFO] Starting session"

# 2º Creating user
session_id=$(curl -s "$METABASE_HOST/api/setup/" \
  --request POST \
  --header 'Content-Type: application/json' \
  --data '{
  "prefs": {
    "site_locale": "en",
    "site_name": "meta"
  },
  "token": '$setup_token',
  "user": {
    "email": "data_forall@email.com",
    "first_name": "Data",
    "last_name": "For All",
    "password": "StrongPassword123"
  }
}' | jq -r '.id')

if [ "$session_id" = "null" ] || [ -z "$session_id" ]; then
  echo "$(date "+%Y-%m-%d %T") [ERROR] Setup fail. Please check if Metabase change "/api/setup/" params."
  exit 1
fi

echo "$(date "+%Y-%m-%d %T") [INFO] Setup done. Creating storage connection"

# 3º Creating storage connection
response=$(curl -s -X POST "$METABASE_HOST/api/database" \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $session_id" \
  -d '{
    "name": "Events Storage",
    "engine": "postgres",
    "details": {
      "host": "events_storage",
      "port": 5432,
      "dbname": "events_storage",
      "user": "user",
      "password": "password",
      "ssl": false
    },
    "is_full_sync": true,
    "is_on_demand": false
  }')

if echo "$response" | grep -qi 'unauthenticated\|error\|permission'; then
  echo "$(date "+%Y-%m-%d %T") [ERROR] Storage connection failed. Reason: $response"
  exit 1
fi

echo "$(date "+%Y-%m-%d %T") [INFO] Storage connection done. Finishing init setup"
exit 0