#!/bin/bash
# Create AWS resources (S3 buckets + DynamoDB tables) for Bedrock DQ Agent
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a; source "$PROJECT_DIR/.env"; set +a
fi

REGION="${AWS_REGION:-us-east-1}"

# ---- S3 Buckets ----
S3_STAGING="${S3_STAGING_BUCKET:?Error: Set S3_STAGING_BUCKET in .env}"
S3_REPORTS="${S3_REPORT_BUCKET:-}"
S3_RULES="${S3_RULES_BUCKET:-}"

echo "=== Creating S3 Buckets (region: $REGION) ==="

create_bucket() {
    local bucket="$1"
    if [ -z "$bucket" ]; then return; fi
    if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        echo "  [EXISTS] s3://$bucket"
    else
        if [ "$REGION" = "us-east-1" ]; then
            aws s3api create-bucket --bucket "$bucket" --region "$REGION"
        else
            aws s3api create-bucket --bucket "$bucket" --region "$REGION" \
                --create-bucket-configuration LocationConstraint="$REGION"
        fi
        echo "  [CREATED] s3://$bucket"
    fi
}

create_bucket "$S3_STAGING"
create_bucket "$S3_REPORTS"
create_bucket "$S3_RULES"

# ---- Upload default rules to S3 ----
RULES_KEY="${S3_RULES_KEY:-rules/delivery_rules.yaml}"
if [ -n "$S3_RULES" ] && [ -f "$PROJECT_DIR/agent/config/rules/delivery_rules.yaml" ]; then
    echo ""
    echo "=== Uploading default rules ==="
    aws s3 cp "$PROJECT_DIR/agent/config/rules/delivery_rules.yaml" \
        "s3://$S3_RULES/$RULES_KEY"
    echo "  [OK] s3://$S3_RULES/$RULES_KEY"
fi

# ---- DynamoDB Tables ----
echo ""
echo "=== Creating DynamoDB Tables (region: $REGION) ==="

TABLE_DATA="${DYNAMODB_TABLE_NAME:-delivery-data-dev}"
TABLE_STATE="${DYNAMODB_STATE_TABLE:-dq-agent-state-dev}"
TABLE_CORRECTIONS="${DYNAMODB_CORRECTION_TABLE:-dq-agent-corrections-dev}"
TABLE_CACHE="${DYNAMODB_CACHE_TABLE:-dq-agent-cache-dev}"
TABLE_QUARANTINE="${DYNAMODB_QUARANTINE_TABLE:-dq-agent-quarantine-dev}"

create_table_simple() {
    local table="$1" pk="$2"
    if aws dynamodb describe-table --table-name "$table" --region "$REGION" &>/dev/null; then
        echo "  [EXISTS] $table"
    else
        aws dynamodb create-table \
            --table-name "$table" \
            --attribute-definitions "AttributeName=$pk,AttributeType=S" \
            --key-schema "AttributeName=$pk,KeyType=HASH" \
            --billing-mode PAY_PER_REQUEST \
            --region "$REGION" > /dev/null
        echo "  [CREATED] $table (pk=$pk)"
    fi
}

create_table_composite() {
    local table="$1" pk="$2" sk="$3"
    if aws dynamodb describe-table --table-name "$table" --region "$REGION" &>/dev/null; then
        echo "  [EXISTS] $table"
    else
        aws dynamodb create-table \
            --table-name "$table" \
            --attribute-definitions \
                "AttributeName=$pk,AttributeType=S" \
                "AttributeName=$sk,AttributeType=S" \
            --key-schema \
                "AttributeName=$pk,KeyType=HASH" \
                "AttributeName=$sk,KeyType=RANGE" \
            --billing-mode PAY_PER_REQUEST \
            --region "$REGION" > /dev/null
        echo "  [CREATED] $table (pk=$pk, sk=$sk)"
    fi
}

create_table_simple    "$TABLE_DATA"        "record_id"
create_table_composite "$TABLE_STATE"       "state_key"   "sort_key"
create_table_composite "$TABLE_CORRECTIONS" "pipeline_id" "sort_key"
create_table_simple    "$TABLE_CACHE"       "pattern_key"
create_table_simple    "$TABLE_QUARANTINE"  "record_id"

# Enable TTL on cache table
echo ""
echo "=== Enabling TTL on cache table ==="
aws dynamodb update-time-to-live \
    --table-name "$TABLE_CACHE" \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" \
    --region "$REGION" 2>/dev/null && echo "  [OK] TTL enabled on $TABLE_CACHE" \
    || echo "  [SKIP] TTL already enabled on $TABLE_CACHE"

echo ""
echo "=== AWS resource setup complete ==="
