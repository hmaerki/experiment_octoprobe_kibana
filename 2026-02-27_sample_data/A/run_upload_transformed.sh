#!/bin/bash
# set -euox pipefail
set -euo pipefail
# set -e

ES_HOST="localhost:9200"
ES_USER="elastic"
ES_PASSWORD="91AwngFy"
INDEX_NAME="octoprobe_a"
BASE_DIR="$(cd "$(dirname "$0")" && pwd)/transformed"

# Create index
curl -s -X PUT -u "$ES_USER:$ES_PASSWORD" \
    "http://$ES_HOST/$INDEX_NAME" \
    -H "Content-Type: application/json" \
    -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 0}}' > /dev/null 2>&1 || true

# Upload all JSON files
total=0
success=0

while IFS= read -r -d '' json_file; do
    total=$((total + 1))
    rel_path="${json_file#$BASE_DIR/}"
    doc_id=$(echo "$rel_path" | sed 's/[^a-zA-Z0-9_-]/_/g')
    echo "Uploading: $doc_id $rel_path"
    
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -u "$ES_USER:$ES_PASSWORD" \
        "http://$ES_HOST/$INDEX_NAME/_doc/$doc_id" \
        -H "Content-Type: application/json" \
        -d @"$json_file")
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    
    echo "http_code: $http_code"
    if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
        echo "Error: $body"
    fi

    if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
        success=$((success + 1))
    fi
done < <(find "$BASE_DIR" -type f -name "*.json" -print0)

echo "Uploaded $success/$total files to index '$INDEX_NAME'"
