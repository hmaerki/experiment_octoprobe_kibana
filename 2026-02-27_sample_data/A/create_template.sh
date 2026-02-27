#!/bin/bash
# Create Elasticsearch index template for octoprobe_A

ES_HOST="localhost:9200"
ES_USER="elastic"
ES_PASSWORD="91AwngFy"
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

curl -X PUT \
  -u "$ES_USER:$ES_PASSWORD" \
  "http://$ES_HOST/_index_template/octoprobe_a" \
  -H "Content-Type: application/json" \
  -d @"$BASE_DIR/create_template.json"

echo ""
echo "Template created successfully"
