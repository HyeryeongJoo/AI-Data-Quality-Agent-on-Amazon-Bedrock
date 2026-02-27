#!/bin/bash
# Upload sample delivery data to S3 for testing
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a; source "$PROJECT_DIR/.env"; set +a
fi

REGION="${AWS_REGION:-us-east-1}"
BUCKET="${S3_STAGING_BUCKET:?Error: Set S3_STAGING_BUCKET in .env}"
KEY="${S3_SAMPLE_KEY:-sample/data.jsonl}"
SAMPLE_FILE="$PROJECT_DIR/agent/tests/fixtures/sample_delivery_data.jsonl"

if [ ! -f "$SAMPLE_FILE" ]; then
    echo "Error: Sample data not found at $SAMPLE_FILE"
    exit 1
fi

RECORD_COUNT=$(wc -l < "$SAMPLE_FILE")

echo "=== Uploading Sample Data ==="
echo "  Source: $SAMPLE_FILE ($RECORD_COUNT records)"
echo "  Target: s3://$BUCKET/$KEY"
echo ""

aws s3 cp "$SAMPLE_FILE" "s3://$BUCKET/$KEY" --region "$REGION"

echo ""
echo "  [OK] Sample data uploaded successfully"
echo ""
echo "  You can now:"
echo "    1. Start the web dashboard:  ./web/start.sh"
echo "    2. Load sample data from the UI"
echo "    3. Run validation"
