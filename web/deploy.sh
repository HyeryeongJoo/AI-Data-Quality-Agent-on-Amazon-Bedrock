#!/bin/bash
# Deploy DQ Agent Web to a new EC2 instance via CloudFormation
set -euo pipefail

STACK_NAME="dq-agent-web"
REGION="us-east-1"
S3_BUCKET="dq-agent-web-deploy-163720405317"
S3_KEY="dq-agent-web.tar.gz"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo " DQ Agent Web - Deploy"
echo "============================================"

# 1) Package the application
echo ""
echo "[1/4] Packaging dq-agent-web..."
cd "$SCRIPT_DIR"
tar -czf /tmp/dq-agent-web.tar.gz \
    --exclude='frontend/node_modules' \
    --exclude='frontend/dist' \
    --exclude='backend/__pycache__' \
    --exclude='backend/routers/__pycache__' \
    --exclude='.git' \
    --exclude='path-standalone-stack.yaml' \
    --exclude='deploy.sh' \
    backend/ frontend/
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
aws cloudformation deploy \
    --stack-name "$STACK_NAME" \
    --template-file "$SCRIPT_DIR/dq-agent-web-stack.yaml" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION" \
    --parameter-overrides \
        S3CodeBucket="$S3_BUCKET" \
        S3CodeKey="$S3_KEY"

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
