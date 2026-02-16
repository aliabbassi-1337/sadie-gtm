# Fargate infrastructure for SQS consumers
# Scales to 0 when queue is empty, scales up when messages arrive

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
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

variable "sqs_cloudbeds_enrichment_queue_arn" {
  description = "ARN of the Cloudbeds enrichment SQS queue"
  type        = string
}

variable "sqs_detection_queue_arn" {
  description = "ARN of the Detection SQS queue"
  type        = string
}

variable "sqs_mews_enrichment_queue_arn" {
  description = "ARN of the Mews enrichment SQS queue"
  type        = string
}

variable "sqs_siteminder_enrichment_queue_arn" {
  description = "ARN of the SiteMinder enrichment SQS queue"
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
          "sqs:SendMessage",
          "sqs:SendMessageBatch",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl"
        ]
        Resource = [
          var.sqs_rms_enrichment_queue_arn,
          var.sqs_rms_scan_queue_arn,
          var.sqs_cloudbeds_enrichment_queue_arn,
          var.sqs_detection_queue_arn,
          var.sqs_mews_enrichment_queue_arn,
          var.sqs_siteminder_enrichment_queue_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::sadie-gtm-exports",
          "arn:aws:s3:::sadie-gtm-exports/*"
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
    
    command = ["uv", "run", "python", "-m", "workflows.enrich_rms_consumer", "--concurrency", "100"]
    
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
    
    command = ["uv", "run", "python", "-m", "workflows.consume_rms_scan", "--concurrency", "100"]
    
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

# =============================================================================
# CLOUDBEDS SQS CONSUMER (scales based on queue depth)
# =============================================================================

# ECS Task Definition - Cloudbeds SQS Consumer
resource "aws_ecs_task_definition" "cloudbeds_consumer" {
  family                   = "${var.app_name}-cloudbeds-consumer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "consumer"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "-m", "workflows.enrich_cloudbeds_consumer", "--concurrency", "100"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-cloudbeds-enrichment-queue-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_ZONE", valueFrom = "/${var.app_name}/brightdata-dc-zone" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "cloudbeds-consumer"
      }
    }
  }])
}

# ECS Service - Cloudbeds Consumer (scales based on SQS)
resource "aws_ecs_service" "cloudbeds_consumer" {
  name            = "${var.app_name}-cloudbeds-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.cloudbeds_consumer.arn
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

# Auto-scaling for Cloudbeds Consumer
resource "aws_appautoscaling_target" "cloudbeds_consumer" {
  max_capacity       = 5
  min_capacity       = 0
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.cloudbeds_consumer.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "cloudbeds_consumer_scale_up" {
  name               = "${var.app_name}-cloudbeds-consumer-scale-up"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.cloudbeds_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.cloudbeds_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.cloudbeds_consumer.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = 60
    metric_aggregation_type = "Average"

    step_adjustment {
      scaling_adjustment          = 1
      metric_interval_lower_bound = 0
      metric_interval_upper_bound = 100
    }
    step_adjustment {
      scaling_adjustment          = 3
      metric_interval_lower_bound = 100
    }
  }
}

resource "aws_appautoscaling_policy" "cloudbeds_consumer_scale_down" {
  name               = "${var.app_name}-cloudbeds-consumer-scale-down"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.cloudbeds_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.cloudbeds_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.cloudbeds_consumer.service_namespace

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

# CloudWatch Alarms for Cloudbeds queue scaling
resource "aws_cloudwatch_metric_alarm" "cloudbeds_queue_high" {
  alarm_name          = "${var.app_name}-cloudbeds-queue-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale up when messages in Cloudbeds queue"

  dimensions = {
    QueueName = "sadie-gtm-cloudbeds-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.cloudbeds_consumer_scale_up.arn]
}

resource "aws_cloudwatch_metric_alarm" "cloudbeds_queue_low" {
  alarm_name          = "${var.app_name}-cloudbeds-queue-empty"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 5
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale down when Cloudbeds queue is empty"

  dimensions = {
    QueueName = "sadie-gtm-cloudbeds-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.cloudbeds_consumer_scale_down.arn]
}

# =============================================================================
# MEWS SQS CONSUMER (scales based on queue depth)
# =============================================================================

# ECS Task Definition - Mews SQS Consumer
resource "aws_ecs_task_definition" "mews_consumer" {
  family                   = "${var.app_name}-mews-consumer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "consumer"
    image = "${var.ecr_repo_url}:latest"

    command = ["uv", "run", "python", "-m", "workflows.enrich_mews_consumer", "--concurrency", "10"]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_MEWS_ENRICHMENT_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-mews-enrichment-queue-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_ZONE", valueFrom = "/${var.app_name}/brightdata-dc-zone" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "mews-consumer"
      }
    }
  }])
}

# ECS Service - Mews Consumer (scales based on SQS)
resource "aws_ecs_service" "mews_consumer" {
  name            = "${var.app_name}-mews-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.mews_consumer.arn
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

# Auto-scaling for Mews Consumer
resource "aws_appautoscaling_target" "mews_consumer" {
  max_capacity       = 3
  min_capacity       = 0
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.mews_consumer.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "mews_consumer_scale_up" {
  name               = "${var.app_name}-mews-consumer-scale-up"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.mews_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.mews_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.mews_consumer.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = 60
    metric_aggregation_type = "Average"

    step_adjustment {
      scaling_adjustment          = 1
      metric_interval_lower_bound = 0
      metric_interval_upper_bound = 100
    }
    step_adjustment {
      scaling_adjustment          = 2
      metric_interval_lower_bound = 100
    }
  }
}

resource "aws_appautoscaling_policy" "mews_consumer_scale_down" {
  name               = "${var.app_name}-mews-consumer-scale-down"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.mews_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.mews_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.mews_consumer.service_namespace

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

# CloudWatch Alarms for Mews queue scaling
resource "aws_cloudwatch_metric_alarm" "mews_queue_high" {
  alarm_name          = "${var.app_name}-mews-queue-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale up when messages in Mews queue"

  dimensions = {
    QueueName = "sadie-gtm-mews-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.mews_consumer_scale_up.arn]
}

resource "aws_cloudwatch_metric_alarm" "mews_queue_low" {
  alarm_name          = "${var.app_name}-mews-queue-empty"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 5
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale down when Mews queue is empty"

  dimensions = {
    QueueName = "sadie-gtm-mews-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.mews_consumer_scale_down.arn]
}

# =============================================================================
# SITEMINDER SQS CONSUMER (scales based on queue depth)
# =============================================================================

# ECS Task Definition - SiteMinder SQS Consumer
resource "aws_ecs_task_definition" "siteminder_consumer" {
  family                   = "${var.app_name}-siteminder-consumer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "consumer"
    image = "${var.ecr_repo_url}:latest"

    command = ["uv", "run", "python", "-m", "workflows.enrich_siteminder_consumer", "--concurrency", "50", "--brightdata"]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_SITEMINDER_ENRICHMENT_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-siteminder-enrichment-queue-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_ZONE", valueFrom = "/${var.app_name}/brightdata-dc-zone" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "siteminder-consumer"
      }
    }
  }])
}

# ECS Service - SiteMinder Consumer (scales based on SQS)
resource "aws_ecs_service" "siteminder_consumer" {
  name            = "${var.app_name}-siteminder-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.siteminder_consumer.arn
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

# Auto-scaling for SiteMinder Consumer
resource "aws_appautoscaling_target" "siteminder_consumer" {
  max_capacity       = 3
  min_capacity       = 0
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.siteminder_consumer.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "siteminder_consumer_scale_up" {
  name               = "${var.app_name}-siteminder-consumer-scale-up"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.siteminder_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.siteminder_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.siteminder_consumer.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = 60
    metric_aggregation_type = "Average"

    step_adjustment {
      scaling_adjustment          = 1
      metric_interval_lower_bound = 0
      metric_interval_upper_bound = 100
    }
    step_adjustment {
      scaling_adjustment          = 2
      metric_interval_lower_bound = 100
    }
  }
}

resource "aws_appautoscaling_policy" "siteminder_consumer_scale_down" {
  name               = "${var.app_name}-siteminder-consumer-scale-down"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.siteminder_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.siteminder_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.siteminder_consumer.service_namespace

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

# CloudWatch Alarms for SiteMinder queue scaling
resource "aws_cloudwatch_metric_alarm" "siteminder_queue_high" {
  alarm_name          = "${var.app_name}-siteminder-queue-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale up when messages in SiteMinder queue"

  dimensions = {
    QueueName = "sadie-gtm-siteminder-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.siteminder_consumer_scale_up.arn]
}

resource "aws_cloudwatch_metric_alarm" "siteminder_queue_low" {
  alarm_name          = "${var.app_name}-siteminder-queue-empty"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 5
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale down when SiteMinder queue is empty"

  dimensions = {
    QueueName = "sadie-gtm-siteminder-enrichment"
  }

  alarm_actions = [aws_appautoscaling_policy.siteminder_consumer_scale_down.arn]
}

# =============================================================================
# DETECTION SQS CONSUMER (scales based on queue depth)
# Uses Playwright for browser-based booking engine detection
# =============================================================================

# ECS Task Definition - Detection Consumer
resource "aws_ecs_task_definition" "detection_consumer" {
  family                   = "${var.app_name}-detection-consumer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "2048"   # 2 vCPU for browser automation
  memory                   = "8192"   # 8GB RAM for Playwright browsers
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "consumer"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "-m", "workflows.detection_consumer", "--pool-size", "10", "--idle-timeout", "120"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" },
      { name = "PLAYWRIGHT_BROWSERS_PATH", value = "/root/.cache/ms-playwright" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_DETECTION_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-detection-queue-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "detection-consumer"
      }
    }
  }])
}

# ECS Service - Detection Consumer (scales based on SQS)
resource "aws_ecs_service" "detection_consumer" {
  name            = "${var.app_name}-detection-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.detection_consumer.arn
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

# Auto-scaling for Detection Consumer
resource "aws_appautoscaling_target" "detection_consumer" {
  max_capacity       = 10
  min_capacity       = 0
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.detection_consumer.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "detection_consumer_scale_up" {
  name               = "${var.app_name}-detection-consumer-scale-up"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.detection_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.detection_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.detection_consumer.service_namespace

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

resource "aws_appautoscaling_policy" "detection_consumer_scale_down" {
  name               = "${var.app_name}-detection-consumer-scale-down"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.detection_consumer.resource_id
  scalable_dimension = aws_appautoscaling_target.detection_consumer.scalable_dimension
  service_namespace  = aws_appautoscaling_target.detection_consumer.service_namespace

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

# CloudWatch Alarms for Detection queue scaling
resource "aws_cloudwatch_metric_alarm" "detection_queue_high" {
  alarm_name          = "${var.app_name}-detection-queue-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale up when messages in detection queue"

  dimensions = {
    QueueName = "detection-queue"
  }

  alarm_actions = [aws_appautoscaling_policy.detection_consumer_scale_up.arn]
}

resource "aws_cloudwatch_metric_alarm" "detection_queue_low" {
  alarm_name          = "${var.app_name}-detection-queue-empty"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 5
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Scale down when detection queue is empty"

  dimensions = {
    QueueName = "detection-queue"
  }

  alarm_actions = [aws_appautoscaling_policy.detection_consumer_scale_down.arn]
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

# =============================================================================
# SCHEDULED ENRICHMENT TASKS (EventBridge -> Fargate)
# =============================================================================

# IAM Role for EventBridge to run ECS tasks
resource "aws_iam_role" "eventbridge_ecs" {
  name = "${var.app_name}-eventbridge-ecs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_ecs" {
  name = "${var.app_name}-eventbridge-ecs-policy"
  role = aws_iam_role.eventbridge_ecs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        Resource = ["arn:aws:ecs:eu-north-1:*:task-definition/${var.app_name}-*"]
      },
      {
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_task_execution.arn,
          aws_iam_role.ecs_task.arn
        ]
      }
    ]
  })
}

# Task Definition - Cloudbeds Enrichment (scheduled)
resource "aws_ecs_task_definition" "cloudbeds_enrichment" {
  family                   = "${var.app_name}-cloudbeds-enrichment"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enrichment"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enrich_booking_engines.py", "cloudbeds", "--limit", "2000", "--concurrency", "100"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_ZONE", valueFrom = "/${var.app_name}/brightdata-dc-zone" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "cloudbeds-enrichment"
      }
    }
  }])
}

# Task Definition - RMS API Enrichment (scheduled)
resource "aws_ecs_task_definition" "rms_api_enrichment" {
  family                   = "${var.app_name}-rms-api-enrichment"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enrichment"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enrich_booking_engines.py", "rms", "--limit", "1000", "--concurrency", "100"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_ZONE", valueFrom = "/${var.app_name}/brightdata-dc-zone" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "rms-api-enrichment"
      }
    }
  }])
}

# Task Definition - SiteMinder Enrichment (scheduled)
resource "aws_ecs_task_definition" "siteminder_enrichment" {
  family                   = "${var.app_name}-siteminder-enrichment"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enrichment"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enrich_booking_engines.py", "siteminder", "--limit", "1000", "--concurrency", "100"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "BRIGHTDATA_CUSTOMER_ID", valueFrom = "/${var.app_name}/brightdata-customer-id" },
      { name = "BRIGHTDATA_DC_ZONE", valueFrom = "/${var.app_name}/brightdata-dc-zone" },
      { name = "BRIGHTDATA_DC_PASSWORD", valueFrom = "/${var.app_name}/brightdata-dc-password" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "siteminder-enrichment"
      }
    }
  }])
}

# Task Definition - Proximity Calculation (scheduled)
resource "aws_ecs_task_definition" "proximity_enrichment" {
  family                   = "${var.app_name}-proximity-enrichment"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enrichment"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enrich_booking_engines.py", "proximity", "--limit", "1000"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "proximity-enrichment"
      }
    }
  }])
}

# EventBridge Rule - Cloudbeds Enrichment (every 10 min)
resource "aws_cloudwatch_event_rule" "cloudbeds_enrichment" {
  name                = "${var.app_name}-cloudbeds-enrichment"
  description         = "Trigger Cloudbeds enrichment every 10 minutes"
  schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "cloudbeds_enrichment" {
  rule      = aws_cloudwatch_event_rule.cloudbeds_enrichment.name
  target_id = "cloudbeds-enrichment"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.cloudbeds_enrichment.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# EventBridge Rule - RMS API Enrichment (every 15 min)
resource "aws_cloudwatch_event_rule" "rms_api_enrichment" {
  name                = "${var.app_name}-rms-api-enrichment"
  description         = "Trigger RMS API enrichment every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "rms_api_enrichment" {
  rule      = aws_cloudwatch_event_rule.rms_api_enrichment.name
  target_id = "rms-api-enrichment"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.rms_api_enrichment.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# EventBridge Rule - SiteMinder Enrichment (every 15 min)
resource "aws_cloudwatch_event_rule" "siteminder_enrichment" {
  name                = "${var.app_name}-siteminder-enrichment"
  description         = "Trigger SiteMinder enrichment every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "siteminder_enrichment" {
  rule      = aws_cloudwatch_event_rule.siteminder_enrichment.name
  target_id = "siteminder-enrichment"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.siteminder_enrichment.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# EventBridge Rule - Proximity Enrichment (every 5 min)
resource "aws_cloudwatch_event_rule" "proximity_enrichment" {
  name                = "${var.app_name}-proximity-enrichment"
  description         = "Trigger proximity calculation every 5 minutes"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "proximity_enrichment" {
  rule      = aws_cloudwatch_event_rule.proximity_enrichment.name
  target_id = "proximity-enrichment"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.proximity_enrichment.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# ENQUEUE TASKS (fill SQS queues for consumers)
# =============================================================================

# Task Definition - Cloudbeds Enqueue
resource "aws_ecs_task_definition" "cloudbeds_enqueue" {
  family                   = "${var.app_name}-cloudbeds-enqueue"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enqueue"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enrich_cloudbeds_enqueue.py", "--limit", "5000"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-cloudbeds-enrichment-queue-url" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "cloudbeds-enqueue"
      }
    }
  }])
}

# EventBridge Rule - Cloudbeds Enqueue (every 15 min)
resource "aws_cloudwatch_event_rule" "cloudbeds_enqueue" {
  name                = "${var.app_name}-cloudbeds-enqueue"
  description         = "Enqueue Cloudbeds hotels for enrichment every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "cloudbeds_enqueue" {
  rule      = aws_cloudwatch_event_rule.cloudbeds_enqueue.name
  target_id = "cloudbeds-enqueue"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.cloudbeds_enqueue.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# Task Definition - RMS Enqueue
resource "aws_ecs_task_definition" "rms_enqueue" {
  family                   = "${var.app_name}-rms-enqueue"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enqueue"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enrich_rms_enqueue.py", "--limit", "5000"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_RMS_ENRICHMENT_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-rms-enrichment-queue-url" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "rms-enqueue"
      }
    }
  }])
}

# Task Definition - Detection Enqueue
resource "aws_ecs_task_definition" "detection_enqueue" {
  family                   = "${var.app_name}-detection-enqueue"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enqueue"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/enqueue_detection.py", "--limit", "5000"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "SQS_DETECTION_QUEUE_URL", valueFrom = "/${var.app_name}/sqs-detection-queue-url" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "detection-enqueue"
      }
    }
  }])
}

# EventBridge Rule - Detection Enqueue (every 30 min)
resource "aws_cloudwatch_event_rule" "detection_enqueue" {
  name                = "${var.app_name}-detection-enqueue"
  description         = "Enqueue hotels for detection every 30 minutes"
  schedule_expression = "rate(30 minutes)"
}

resource "aws_cloudwatch_event_target" "detection_enqueue" {
  rule      = aws_cloudwatch_event_rule.detection_enqueue.name
  target_id = "detection-enqueue"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.detection_enqueue.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# EventBridge Rule - RMS Enqueue (every 4 hours)
resource "aws_cloudwatch_event_rule" "rms_enqueue" {
  name                = "${var.app_name}-rms-enqueue"
  description         = "Enqueue RMS hotels for enrichment every 4 hours"
  schedule_expression = "rate(4 hours)"
}

resource "aws_cloudwatch_event_target" "rms_enqueue" {
  rule      = aws_cloudwatch_event_rule.rms_enqueue.name
  target_id = "rms-enqueue"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.rms_enqueue.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# ROOM COUNT ENRICHMENT (Azure OpenAI)
# =============================================================================

resource "aws_ecs_task_definition" "room_count_enrichment" {
  family                   = "${var.app_name}-room-count-enrichment"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "enrichment"
    image = "${var.ecr_repo_url}:latest"

    command = ["uv", "run", "python", "workflows/enrichment.py", "room-counts", "--limit", "100"]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "AZURE_OPENAI_API_KEY", valueFrom = "/${var.app_name}/azure-openai-api-key" },
      { name = "AZURE_OPENAI_ENDPOINT", valueFrom = "/${var.app_name}/azure-openai-endpoint" },
      { name = "AZURE_OPENAI_DEPLOYMENT", valueFrom = "/${var.app_name}/azure-openai-deployment" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "room-count-enrichment"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "room_count_enrichment" {
  name                = "${var.app_name}-room-count-enrichment"
  description         = "Enrich hotels with room counts (disabled - run manually)"
  schedule_expression = "rate(2 minutes)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "room_count_enrichment" {
  rule      = aws_cloudwatch_event_rule.room_count_enrichment.name
  target_id = "room-count-enrichment"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.room_count_enrichment.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# LAUNCHER (mark fully enriched hotels as live)
# =============================================================================

resource "aws_ecs_task_definition" "launcher" {
  family                   = "${var.app_name}-launcher"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "launcher"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/launcher.py", "launch-all"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "launcher"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "launcher" {
  name                = "${var.app_name}-launcher"
  description         = "Launch fully enriched hotels every 2 minutes"
  schedule_expression = "rate(2 minutes)"
}

resource "aws_cloudwatch_event_target" "launcher" {
  rule      = aws_cloudwatch_event_rule.launcher.name
  target_id = "launcher"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.launcher.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# EXPORTS (S3)
# =============================================================================

resource "aws_ecs_task_definition" "export_states" {
  family                   = "${var.app_name}-export-states"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "export"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/export.py", "--all"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "AWS_ACCESS_KEY_ID", valueFrom = "/${var.app_name}/aws-access-key-id" },
      { name = "AWS_SECRET_ACCESS_KEY", valueFrom = "/${var.app_name}/aws-secret-access-key" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "export-states"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "export_states" {
  name                = "${var.app_name}-export-states"
  description         = "Export all state reports to S3 every 6 hours"
  schedule_expression = "rate(6 hours)"
}

resource "aws_cloudwatch_event_target" "export_states" {
  rule      = aws_cloudwatch_event_rule.export_states.name
  target_id = "export-states"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.export_states.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

resource "aws_ecs_task_definition" "export_crawl" {
  family                   = "${var.app_name}-export-crawl"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "export"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "-m", "workflows.export_crawl", "--all"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "AWS_ACCESS_KEY_ID", valueFrom = "/${var.app_name}/aws-access-key-id" },
      { name = "AWS_SECRET_ACCESS_KEY", valueFrom = "/${var.app_name}/aws-secret-access-key" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "export-crawl"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "export_crawl" {
  name                = "${var.app_name}-export-crawl"
  description         = "Export crawl data by booking engine every 6 hours"
  schedule_expression = "rate(6 hours)"
}

resource "aws_cloudwatch_event_target" "export_crawl" {
  rule      = aws_cloudwatch_event_rule.export_crawl.name
  target_id = "export-crawl"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.export_crawl.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# NORMALIZE STATES (one-time or periodic cleanup)
# =============================================================================

resource "aws_ecs_task_definition" "normalize_states" {
  family                   = "${var.app_name}-normalize-states"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "normalize"
    image = "${var.ecr_repo_url}:latest"
    
    command = ["uv", "run", "python", "workflows/normalize_states.py"]
    
    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]
    
    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" }
    ]
    
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "normalize-states"
      }
    }
  }])
}

# Note: normalize_states is run on-demand, not scheduled
# To run manually: aws ecs run-task --cluster sadie-gtm-cluster --task-definition sadie-gtm-normalize-states ...

# =============================================================================
# NORMALIZE LOCATIONS (country codes, state abbreviations, special chars)
# =============================================================================

resource "aws_ecs_task_definition" "normalize_locations" {
  family                   = "${var.app_name}-normalize-locations"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "normalize"
    image = "${var.ecr_repo_url}:latest"

    command = ["sh", "-c", "uv run python -m workflows.normalize_data && uv run python -m workflows.normalize"]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "normalize-locations"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "normalize_locations" {
  name                = "${var.app_name}-normalize-locations"
  description         = "Normalize country codes, state abbreviations, and special chars every 6 hours"
  schedule_expression = "rate(6 hours)"
}

resource "aws_cloudwatch_event_target" "normalize_locations" {
  rule      = aws_cloudwatch_event_rule.normalize_locations.name
  target_id = "normalize-locations"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.normalize_locations.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# CHECK ACTIVE (property active status detection)
# =============================================================================

resource "aws_ecs_task_definition" "check_active" {
  family                   = "${var.app_name}-check-active"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "check-active"
    image = "${var.ecr_repo_url}:latest"

    command = ["uv", "run", "python", "-m", "workflows.check_active", "--limit", "2000", "--concurrency", "200", "--rpm", "1000"]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" },
      { name = "AZURE_OPENAI_API_KEY", valueFrom = "/${var.app_name}/azure-openai-api-key" },
      { name = "AZURE_OPENAI_ENDPOINT", valueFrom = "/${var.app_name}/azure-openai-endpoint" },
      { name = "AZURE_OPENAI_DEPLOYMENT", valueFrom = "/${var.app_name}/azure-openai-deployment" },
      { name = "SERPER_API_KEY", valueFrom = "/${var.app_name}/serper-api-key" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "check-active"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "check_active" {
  name                = "${var.app_name}-check-active"
  description         = "Check hotel active status every 12 hours"
  schedule_expression = "rate(12 hours)"
  is_enabled          = false
}

resource "aws_cloudwatch_event_target" "check_active" {
  rule      = aws_cloudwatch_event_rule.check_active.name
  target_id = "check-active"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.check_active.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
}

# =============================================================================
# RMS AVAILABILITY (booking availability enrichment)
# =============================================================================

resource "aws_ecs_task_definition" "rms_availability" {
  family                   = "${var.app_name}-rms-availability"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "rms-availability"
    image = "${var.ecr_repo_url}:latest"

    command = ["uv", "run", "python", "-m", "workflows.rms_availability", "--limit", "4000", "--concurrency", "30"]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.consumer.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "rms-availability"
      }
    }
  }])
}

resource "aws_cloudwatch_event_rule" "rms_availability" {
  name                = "${var.app_name}-rms-availability"
  description         = "Check RMS hotel availability every 7 days"
  schedule_expression = "rate(7 days)"
  is_enabled          = false
}

resource "aws_cloudwatch_event_target" "rms_availability" {
  rule      = aws_cloudwatch_event_rule.rms_availability.name
  target_id = "rms-availability"
  arn       = aws_ecs_cluster.main.arn
  role_arn  = aws_iam_role.eventbridge_ecs.arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.rms_availability.arn
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      security_groups  = [aws_security_group.fargate.id]
      assign_public_ip = true
    }
  }
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

output "cloudbeds_consumer_service_name" {
  value = aws_ecs_service.cloudbeds_consumer.name
}

output "detection_consumer_service_name" {
  value = aws_ecs_service.detection_consumer.name
}

output "mews_consumer_service_name" {
  value = aws_ecs_service.mews_consumer.name
}

output "siteminder_consumer_service_name" {
  value = aws_ecs_service.siteminder_consumer.name
}

output "scheduled_tasks" {
  value = {
    # Enrichment tasks
    cloudbeds_enrichment  = aws_cloudwatch_event_rule.cloudbeds_enrichment.name
    rms_api_enrichment    = aws_cloudwatch_event_rule.rms_api_enrichment.name
    siteminder_enrichment = aws_cloudwatch_event_rule.siteminder_enrichment.name
    proximity_enrichment  = aws_cloudwatch_event_rule.proximity_enrichment.name
    room_count_enrichment = aws_cloudwatch_event_rule.room_count_enrichment.name
    
    # Enqueue tasks
    cloudbeds_enqueue = aws_cloudwatch_event_rule.cloudbeds_enqueue.name
    rms_enqueue       = aws_cloudwatch_event_rule.rms_enqueue.name
    detection_enqueue = aws_cloudwatch_event_rule.detection_enqueue.name
    
    # Launcher
    launcher = aws_cloudwatch_event_rule.launcher.name
    
    # Exports
    export_states = aws_cloudwatch_event_rule.export_states.name
    export_crawl  = aws_cloudwatch_event_rule.export_crawl.name

    # Normalization
    normalize_locations = aws_cloudwatch_event_rule.normalize_locations.name

    # Active status
    check_active = aws_cloudwatch_event_rule.check_active.name

    # Availability
    rms_availability = aws_cloudwatch_event_rule.rms_availability.name
  }
}

output "on_demand_tasks" {
  description = "Tasks that can be run manually via aws ecs run-task"
  value = {
    normalize_states = aws_ecs_task_definition.normalize_states.family
  }
}
