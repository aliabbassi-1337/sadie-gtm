#!/bin/bash
set -e

# Deploy consumer to AWS Fargate
# Usage: ./scripts/deploy_fargate.sh

AWS_REGION="eu-north-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO="sadie-gtm-consumer"
IMAGE_TAG="latest"

echo "=== Building and deploying to Fargate ==="

# 1. Create ECR repo if not exists
echo "Creating ECR repository..."
aws ecr describe-repositories --repository-names $ECR_REPO --region $AWS_REGION 2>/dev/null || \
    aws ecr create-repository --repository-name $ECR_REPO --region $AWS_REGION

# 2. Login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# 3. Build image
echo "Building Docker image..."
docker build --platform linux/amd64 -f Dockerfile.consumer -t $ECR_REPO:$IMAGE_TAG .

# 4. Tag and push
echo "Pushing to ECR..."
docker tag $ECR_REPO:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$IMAGE_TAG
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$IMAGE_TAG

echo "=== Image pushed to ECR ==="
echo "ECR URL: $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$IMAGE_TAG"
echo ""
echo "Next steps:"
echo "1. Set up SSM Parameter Store secrets (database URL, Brightdata creds)"
echo "2. Run: cd infra && terraform init && terraform apply"
echo "3. Services will auto-scale based on SQS queue depth"
