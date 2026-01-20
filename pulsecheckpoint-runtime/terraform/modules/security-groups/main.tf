variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "allowed_cidr_blocks" {
  type    = list(string)
  default = []
}

variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_security_group" "pulse_runtime" {
  name        = "pulse-${var.environment}-runtime"
  description = "Security group for PulseCheckpoint runtime"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 50051
    to_port     = 50051
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "gRPC API"
  }

  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
    description = "Metrics endpoint"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = merge(var.tags, {
    Name = "pulse-${var.environment}-runtime"
  })
}

output "security_group_id" {
  value = aws_security_group.pulse_runtime.id
}
