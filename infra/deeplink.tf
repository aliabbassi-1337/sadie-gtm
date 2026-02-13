# =============================================================================
# DEEP-LINK SERVICE — always-on Fargate service behind ALB
# =============================================================================

variable "deeplink_ecr_repo_url" {
  description = "ECR repository URL for deeplink service image"
  type        = string
}

# ---------------------------------------------------------------------------
# ECR
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "deeplink" {
  name                 = "${var.app_name}-deeplink"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "deeplink" {
  name              = "/ecs/${var.app_name}-deeplink"
  retention_in_days = 7
}

# ---------------------------------------------------------------------------
# Task Definition
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "deeplink" {
  family                   = "${var.app_name}-deeplink"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "deeplink"
    image = "${var.deeplink_ecr_repo_url}:latest"

    portMappings = [{
      containerPort = 8000
      protocol      = "tcp"
    }]

    environment = [
      { name = "AWS_REGION", value = "eu-north-1" }
    ]

    secrets = [
      { name = "DATABASE_URL", valueFrom = "/${var.app_name}/database-url" }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.deeplink.name
        "awslogs-region"        = "eu-north-1"
        "awslogs-stream-prefix" = "deeplink"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:8000/api/deeplink')\" || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 10
    }
  }])
}

# ---------------------------------------------------------------------------
# Subnets — one per AZ (ALB requires unique AZs)
# ---------------------------------------------------------------------------

data "aws_subnets" "default_one_per_az" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# ---------------------------------------------------------------------------
# ALB
# ---------------------------------------------------------------------------

resource "aws_security_group" "deeplink_alb" {
  name        = "${var.app_name}-deeplink-alb"
  description = "Allow inbound HTTP/HTTPS to deeplink ALB"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "deeplink_task" {
  name        = "${var.app_name}-deeplink-task"
  description = "Allow ALB to reach deeplink Fargate task"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.deeplink_alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb" "deeplink" {
  name               = "${var.app_name}-deeplink"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.deeplink_alb.id]
  subnets            = data.aws_subnets.default_one_per_az.ids
}

resource "aws_lb_target_group" "deeplink" {
  name        = "${var.app_name}-deeplink"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  health_check {
    path                = "/r/healthz"
    port                = "traffic-port"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 30
    matcher             = "200-499" # 404 is fine — means the app is up
  }
}

# HTTP listener — redirect to HTTPS when cert is ready, serve directly for now
resource "aws_lb_listener" "deeplink_http" {
  load_balancer_arn = aws_lb.deeplink.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.deeplink.arn
  }
}

# Uncomment when ACM certificate is provisioned:
#
# variable "deeplink_acm_cert_arn" {
#   description = "ACM certificate ARN for the deeplink domain"
#   type        = string
# }
#
# resource "aws_lb_listener" "deeplink_https" {
#   load_balancer_arn = aws_lb.deeplink.arn
#   port              = 443
#   protocol          = "HTTPS"
#   ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
#   certificate_arn   = var.deeplink_acm_cert_arn
#
#   default_action {
#     type             = "forward"
#     target_group_arn = aws_lb_target_group.deeplink.arn
#   }
# }
#
# # Redirect HTTP → HTTPS (replace the http listener above)
# resource "aws_lb_listener" "deeplink_http_redirect" {
#   load_balancer_arn = aws_lb.deeplink.arn
#   port              = 80
#   protocol          = "HTTP"
#
#   default_action {
#     type = "redirect"
#     redirect {
#       port        = "443"
#       protocol    = "HTTPS"
#       status_code = "HTTP_301"
#     }
#   }
# }

# ---------------------------------------------------------------------------
# ECS Service
# ---------------------------------------------------------------------------

resource "aws_ecs_service" "deeplink" {
  name            = "${var.app_name}-deeplink"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.deeplink.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.deeplink_task.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.deeplink.arn
    container_name   = "deeplink"
    container_port   = 8000
  }

  depends_on = [aws_lb_listener.deeplink_http]
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "deeplink_alb_dns" {
  description = "ALB DNS name for the deep-link service"
  value       = aws_lb.deeplink.dns_name
}

output "deeplink_ecr_repo" {
  description = "ECR repository URL for deeplink image"
  value       = aws_ecr_repository.deeplink.repository_url
}
