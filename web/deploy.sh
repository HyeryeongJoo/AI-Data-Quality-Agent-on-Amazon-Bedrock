#!/bin/bash
# Deploy DQ Agent Web to a new EC2 instance via CloudFormation
set -euo pipefail

STACK_NAME="${STACK_NAME:-bedrock-dq-agent}"
REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${DEPLOY_S3_BUCKET:?Error: Set DEPLOY_S3_BUCKET environment variable}"
S3_KEY="dq-agent-web.tar.gz"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Optional: AgentCore runtime ARN and S3 staging bucket
AGENT_RUNTIME_ARN="${AGENT_RUNTIME_ARN:-}"
S3_STAGING_BUCKET="${S3_STAGING_BUCKET:-}"

echo "============================================"
echo " Bedrock DQ Agent - Deploy"
echo "============================================"

# 1) Package the application (agent + web)
echo ""
echo "[1/4] Packaging bedrock-dq-agent..."
cd "$PROJECT_DIR"
tar -czf /tmp/dq-agent-web.tar.gz \
    --exclude='web/frontend/node_modules' \
    --exclude='web/frontend/dist' \
    --exclude='web/backend/__pycache__' \
    --exclude='web/backend/routers/__pycache__' \
    --exclude='agent/.venv' \
    --exclude='agent/tests' \
    --exclude='.git' \
    --exclude='web/deploy.sh' \
    agent/ web/backend/ web/frontend/ web/start.sh
echo "   Package created: /tmp/dq-agent-web.tar.gz ($(du -h /tmp/dq-agent-web.tar.gz | cut -f1))"

# 2) Create S3 bucket (ignore error if already exists)
echo ""
echo "[2/4] Ensuring S3 bucket exists: ${S3_BUCKET}"
aws s3api create-bucket --bucket "$S3_BUCKET" --region "$REGION" 2>/dev/null || true

# 3) Upload package to S3
echo ""
echo "[3/4] Uploading package to s3://${S3_BUCKET}/${S3_KEY}..."
aws s3 cp /tmp/dq-agent-web.tar.gz "s3://${S3_BUCKET}/${S3_KEY}"
rm -f /tmp/dq-agent-web.tar.gz
echo "   Upload complete."

# 4) Deploy CloudFormation stack
echo ""
echo "[4/4] Deploying CloudFormation stack: ${STACK_NAME}..."

PARAM_OVERRIDES="S3CodeBucket=$S3_BUCKET S3CodeKey=$S3_KEY"
[ -n "$AGENT_RUNTIME_ARN" ] && PARAM_OVERRIDES="$PARAM_OVERRIDES AgentRuntimeArn=$AGENT_RUNTIME_ARN"
[ -n "$S3_STAGING_BUCKET" ] && PARAM_OVERRIDES="$PARAM_OVERRIDES S3StagingBucket=$S3_STAGING_BUCKET"

aws cloudformation deploy \
    --stack-name "$STACK_NAME" \
    --template-file "$SCRIPT_DIR/../infra/cloudformation.yaml" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION" \
    --parameter-overrides $PARAM_OVERRIDES

echo ""
echo "============================================"
echo " Deployment Complete!"
echo "============================================"

# Print outputs
CF_URL=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`CloudFrontURL`].OutputValue' \
    --output text)

INSTANCE_ID=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`InstanceId`].OutputValue' \
    --output text)

echo ""
echo " CloudFront URL : ${CF_URL}"
echo " EC2 Instance ID: ${INSTANCE_ID}"
echo ""
echo " Test: curl ${CF_URL}/api/health"
