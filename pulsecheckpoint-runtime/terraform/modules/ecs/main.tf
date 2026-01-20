variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "task_execution_role_arn" {
  type = string
}

variable "task_role_arn" {
  type = string
}

variable "container_image" {
  type = string
}

variable "s3_bucket" {
  type = string
}

variable "s3_region" {
  type    = string
  default = "us-east-1"
}

variable "cpu" {
  type    = number
  default = 512
}

variable "memory" {
  type    = number
  default = 1024
}

variable "desired_count" {
  type    = number
  default = 2
}

variable "tags" {
  type    = map(string)
  default = {}
}

# ECS Cluster
resource "aws_ecs_cluster" "pulse" {
  name = "pulse-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = var.tags
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "pulse" {
  name              = "/ecs/pulse-${var.environment}"
  retention_in_days = 30
  tags              = var.tags
}

# Task Definition
resource "aws_ecs_task_definition" "pulse" {
  family                   = "pulse-${var.environment}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.task_execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([
    {
      name      = "pulse-runtime"
      image     = var.container_image
      essential = true

      portMappings = [
        {
          containerPort = 50051
          protocol      = "tcp"
        },
        {
          containerPort = 9090
          protocol      = "tcp"
        }
      ]

      environment = [
        { name = "PULSE_GRPC_ADDR", value = "0.0.0.0:50051" },
        { name = "PULSE_METRICS_ADDR", value = "0.0.0.0:9090" },
        { name = "PULSE_S3_ENDPOINT", value = "https://s3.${var.s3_region}.amazonaws.com" },
        { name = "PULSE_S3_BUCKET", value = var.s3_bucket },
        { name = "PULSE_S3_REGION", value = var.s3_region },
        { name = "PULSE_LOG_LEVEL", value = "info" }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.pulse.name
          "awslogs-region"        = var.s3_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:9090/health || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    }
  ])

  tags = var.tags
}

# ECS Service
resource "aws_ecs_service" "pulse" {
  name            = "pulse-${var.environment}"
  cluster         = aws_ecs_cluster.pulse.id
  task_definition = aws_ecs_task_definition.pulse.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = var.security_group_ids
    assign_public_ip = false
  }

  deployment_configuration {
    minimum_healthy_percent = 50
    maximum_percent         = 200
  }

  tags = var.tags
}

output "cluster_arn" {
  value = aws_ecs_cluster.pulse.arn
}

output "service_name" {
  value = aws_ecs_service.pulse.name
}
