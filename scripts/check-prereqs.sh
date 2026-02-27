#!/bin/bash
# Check all prerequisites for running Bedrock DQ Agent
set -e

PASS=0
FAIL=0

check() {
    local name="$1" cmd="$2" min_version="$3"
    if command -v "$cmd" &>/dev/null; then
        local ver
        ver=$("$cmd" --version 2>&1 | head -1)
        echo "  [OK] $name: $ver"
        ((PASS++))
    else
        echo "  [FAIL] $name: not found (need $min_version+)"
        ((FAIL++))
    fi
}

echo "=== Bedrock DQ Agent — Prerequisites Check ==="
echo ""

echo "1. Runtime"
check "Python"   python3.12  "3.12"
check "Node.js"  node         "22"
check "npm"      npm          "10"

echo ""
echo "2. AWS"
check "AWS CLI"  aws          "2.x"

# Check AWS credentials
if aws sts get-caller-identity &>/dev/null; then
    ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    echo "  [OK] AWS credentials configured (account: $ACCOUNT)"
    ((PASS++))
else
    echo "  [FAIL] AWS credentials not configured — run 'aws configure'"
    ((FAIL++))
fi

# Check Bedrock model access
REGION="${AWS_REGION:-us-east-1}"
if aws bedrock list-foundation-models --region "$REGION" --query 'modelSummaries[0].modelId' --output text &>/dev/null; then
    echo "  [OK] Bedrock access available (region: $REGION)"
    ((PASS++))
else
    echo "  [WARN] Cannot verify Bedrock access in $REGION — ensure your account has model access"
fi

echo ""
echo "3. Configuration"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_DIR/.env" ]; then
    echo "  [OK] .env file exists"
    ((PASS++))

    # Check key variables
    source "$PROJECT_DIR/.env"
    for var in S3_STAGING_BUCKET AWS_REGION; do
        val="${!var}"
        if [ -n "$val" ]; then
            echo "  [OK] $var = $val"
        else
            echo "  [WARN] $var is not set in .env"
        fi
    done
else
    echo "  [FAIL] .env not found — run: cp .env.example .env"
    ((FAIL++))
fi

echo ""
echo "============================================"
echo "  Passed: $PASS   Failed: $FAIL"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "Fix the issues above before proceeding."
    exit 1
else
    echo ""
    echo "All prerequisites met! Run: ./setup.sh"
fi
