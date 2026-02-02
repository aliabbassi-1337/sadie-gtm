#!/bin/bash
set -e

# Quick Fargate deployment using AWS CLI (no Terraform needed)
# Usage: ./scripts/fargate_quick_deploy.sh

AWS_REGION="eu-north-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CLUSTER_NAME="sadie-gtm-cluster"
ECR_REPO="sadie-gtm-consumer"
TASK_FAMILY="sadie-gtm-rms-consumer"

echo "=== Quick Fargate Deploy ==="

# 1. Create ECR repo
echo "1. Creating ECR repository..."
aws ecr describe-repositories --repository-names $ECR_REPO --region $AWS_REGION 2>/dev/null || \
    aws ecr create-repository --repository-name $ECR_REPO --region $AWS_REGION

# 2. Build and push Docker image
echo "2. Building and pushing Docker image..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

docker build -f Dockerfile.consumer -t $ECR_REPO:latest .
docker tag $ECR_REPO:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest

# 3. Create ECS cluster
echo "3. Creating ECS cluster..."
aws ecs create-cluster --cluster-name $CLUSTER_NAME --region $AWS_REGION 2>/dev/null || true

# 4. Create task execution role (if not exists)
echo "4. Setting up IAM roles..."
cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role --role-name ecsTaskExecutionRole --assume-role-policy-document file:///tmp/trust-policy.json 2>/dev/null || true
aws iam attach-role-policy --role-name ecsTaskExecutionRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy 2>/dev/null || true

# 5. Register task definition
echo "5. Registering task definition..."
cat > /tmp/task-def.json << EOF
{
  "family": "$TASK_FAMILY",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "arn:aws:iam::$AWS_ACCOUNT_ID:role/ecsTaskExecutionRole",
  "containerDefinitions": [{
    "name": "consumer",
    "image": "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest",
    "command": ["uv", "run", "python", "-m", "workflows.enrich_rms_consumer", "--concurrency", "50"],
    "environment": [
      {"name": "AWS_REGION", "value": "$AWS_REGION"},
      {"name": "DATABASE_URL", "value": "${DATABASE_URL}"},
      {"name": "SQS_RMS_ENRICHMENT_QUEUE_URL", "value": "${SQS_RMS_ENRICHMENT_QUEUE_URL}"},
      {"name": "BRIGHTDATA_CUSTOMER_ID", "value": "${BRIGHTDATA_CUSTOMER_ID}"},
      {"name": "BRIGHTDATA_DC_PASSWORD", "value": "${BRIGHTDATA_DC_PASSWORD}"}
    ],
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/ecs/sadie-gtm-consumer",
        "awslogs-region": "$AWS_REGION",
        "awslogs-stream-prefix": "ecs",
        "awslogs-create-group": "true"
      }
    }
  }]
}
EOF

aws ecs register-task-definition --cli-input-json file:///tmp/task-def.json --region $AWS_REGION

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "To run a task manually:"
echo "  aws ecs run-task --cluster $CLUSTER_NAME --task-definition $TASK_FAMILY --launch-type FARGATE --network-configuration 'awsvpcConfiguration={subnets=[subnet-xxx],assignPublicIp=ENABLED}' --region $AWS_REGION"
echo ""
echo "To create an auto-scaling service, use the Terraform in infra/fargate.tf"
