# Fargate infrastructure for SQS consumers
# Scales to 0 when queue is empty, scales up when messages arrive

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-north-1"
}

# Variables
variable "app_name" {
  default = "sadie-gtm"
}

variable "ecr_repo_url" {
  description = "ECR repository URL for consumer image"
  type        = string
}

variable "sqs_rms_enrichment_queue_arn" {
  description = "ARN of the RMS enrichment SQS queue"
  type        = string
}

variable "sqs_rms_scan_queue_arn" {
  description = "ARN of the RMS scan SQS queue"
  type        = string
}

# VPC (use default for now)
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${var.app_name}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# IAM Role for ECS Task Execution
resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.app_name}-ecs-task-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM Role for ECS Task (application permissions)
resource "aws_iam_role" "ecs_task" {
  name = "${var.app_name}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_sqs" {
  name = "${var.app_name}-sqs-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl"
        ]
        Resource = [
          var.sqs_rms_enrichment_queue_arn,
          var.sqs_rms_scan_queue_arn
        ]
      }
    ]
  })
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/ecs/${var.app_name}-consumer"
  retention_in_days = 7
}

# ECS Task Definition - RMS Enrichment Consumer
resource "aws_ecs_task_definition" "rms_enrichment" {
  family                   = "${var.app_name}-rms-enrichment"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "consumer"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "-m", "workflows.enrich_rms_consumer", "--concurrency", "50"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    # Secrets from Parameter Store
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_RMS_ENRICHMENT_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-rms-enrichment-queue-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "rms-enrichment"
      }
    }
  }])
}

# ECS Task Definition - RMS Scan Consumer  
resource "aws_ecs_task_definition" "rms_scan" {
  family                   = "${var.app_name}-rms-scan"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "consumer"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "-m", "workflows.consume_rms_scan", "--concurrency", "50"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_RMS_SCAN_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-rms-scan-queue-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "rms-scan"
      }
    }
  }])
}

# Security Group for Fargate tasks
resource "aws_security_group" "fargate" {
  name        = "${var.app_name}-fargate"
  description = "Security group for Fargate tasks"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ECS Service - RMS Enrichment (scales based on SQS)
resource "aws_ecs_service" "rms_enrichment" {
  name            = "${var.app_name}-rms-enrichment"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.rms_enrichment.arn
  desired_count   = 0  # Starts at 0, auto-scaling kicks in
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.fargate.id]
    assign_public_ip = true
  }

  lifecycle {
    ignore_changes = [desired_count]  # Managed by auto-scaling
  }
}

# ECS Service - RMS Scan
resource "aws_ecs_service" "rms_scan" {
  name            = "${var.app_name}-rms-scan"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.rms_scan.arn
  desired_count   = 0
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.fargate.id]
    assign_public_ip = true
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}

# Auto-scaling for RMS Enrichment based on SQS queue depth
resource "aws_appautoscaling_target" "rms_enrichment" {
  max_capacity       = 10
  min_capacity       = 0
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.rms_enrichment.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "rms_enrichment_scale_up" {
  name               = "${var.app_name}-rms-enrichment-scale-up"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.rms_enrichment.resource_id
  scalable_dimension = aws_appautoscaling_target.rms_enrichment.scalable_dimension
  service_namespace  = aws_appautoscaling_target.rms_enrichment.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = 60
    metric_aggregation_type = "Average"

    step_adjustment {
      scaling_adjustment          = 2
      metric_interval_lower_bound = 0
      metric_interval_upper_bound = 100
    }
    step_adjustment {
      scaling_adjustment          = 5
      metric_interval_lower_bound = 100
    }
  }
}

resource "aws_appautoscaling_policy" "rms_enrichment_scale_down" {
  name               = "${var.app_name}-rms-enrichment-scale-down"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.rms_enrichment.resource_id
  scalable_dimension = aws_appautoscaling_target.rms_enrichment.scalable_dimension
  service_namespace  = aws_appautoscaling_target.rms_enrichment.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = 300
    metric_aggregation_type = "Average"

    step_adjustment {
      scaling_adjustment          = -1
      metric_interval_upper_bound = 0
    }
  }
}

# CloudWatch Alarm to trigger scaling based on SQS messages
resource "aws_cloudwatch_metric_alarm" "rms_enrichment_queue_high" {
  alarm_name          = "${var.app_name}-rms-enrichment-queue-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale up when messages in queue"

  dimensions = {
    QueueName = "sadie-gtm-rms-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.rms_enrichment_scale_up.arn]
}

resource "aws_cloudwatch_metric_alarm" "rms_enrichment_queue_low" {
  alarm_name          = "${var.app_name}-rms-enrichment-queue-empty"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 5
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale down when queue is empty"

  dimensions = {
    QueueName = "sadie-gtm-rms-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.rms_enrichment_scale_down.arn]
}

# Outputs
output "cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "rms_enrichment_service_name" {
  value = aws_ecs_service.rms_enrichment.name
}

output "rms_scan_service_name" {
  value = aws_ecs_service.rms_scan.name
}
