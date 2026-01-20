terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "PulseCheckpoint"
      Environment = "dev"
      ManagedBy   = "Terraform"
    }
  }
}

variable "aws_region" {
  default = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID for deployment"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS tasks"
  type        = list(string)
}

locals {
  environment = "dev"
  tags = {
    Project     = "PulseCheckpoint"
    Environment = local.environment
  }
}

module "s3" {
  source      = "../../modules/s3"
  bucket_name = "pulse-checkpoints-${local.environment}-${data.aws_caller_identity.current.account_id}"
  environment = local.environment
  tags        = local.tags
}

module "iam" {
  source        = "../../modules/iam"
  environment   = local.environment
  s3_bucket_arn = module.s3.bucket_arn
  tags          = local.tags
}

module "security_groups" {
  source              = "../../modules/security-groups"
  environment         = local.environment
  vpc_id              = var.vpc_id
  allowed_cidr_blocks = ["10.0.0.0/8"]
  tags                = local.tags
}

module "ecs" {
  source                  = "../../modules/ecs"
  environment             = local.environment
  vpc_id                  = var.vpc_id
  subnet_ids              = var.subnet_ids
  security_group_ids      = [module.security_groups.security_group_id]
  task_execution_role_arn = module.iam.task_execution_role_arn
  task_role_arn           = module.iam.task_role_arn
  container_image         = "ghcr.io/your-org/pulsecheckpoint-runtime:latest"
  s3_bucket               = module.s3.bucket_name
  s3_region               = var.aws_region
  cpu                     = 256
  memory                  = 512
  desired_count           = 1
  tags                    = local.tags
}

data "aws_caller_identity" "current" {}

output "s3_bucket" {
  value = module.s3.bucket_name
}

output "ecs_cluster" {
  value = module.ecs.cluster_arn
}
