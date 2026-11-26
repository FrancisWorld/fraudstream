data "aws_caller_identity" "current" {}

locals {
  http_port       = 8080
  management_port = 8081
  topic_prefix    = "fraud"
  # Kafka Streams names its internal topics and consumer group after the application id.
  application_id = var.name
  topic_arn_base = replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/")
  group_arn_base = replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")
  txn_arn_base   = replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":transactional-id/")
}

# ---------------------------------------------------------------- networking

resource "aws_security_group" "msk" {
  name_prefix = "${var.name}-msk-"
  description = "MSK Serverless: IAM-authenticated clients from the service only"
  vpc_id      = var.vpc_id
}

resource "aws_security_group" "service" {
  name_prefix = "${var.name}-svc-"
  description = "fraudstream tasks"
  vpc_id      = var.vpc_id
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.name}-alb-"
  description = "Internal load balancer"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_service" {
  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  description                  = "Kafka IAM listener"
}

resource "aws_vpc_security_group_ingress_rule" "service_from_alb_api" {
  security_group_id            = aws_security_group.service.id
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = local.http_port
  to_port                      = local.http_port
  ip_protocol                  = "tcp"
  description                  = "API from the load balancer"
}

resource "aws_vpc_security_group_ingress_rule" "service_from_alb_health" {
  security_group_id            = aws_security_group.service.id
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = local.management_port
  to_port                      = local.management_port
  ip_protocol                  = "tcp"
  description                  = "Health checks"
}

resource "aws_vpc_security_group_ingress_rule" "service_peers" {
  security_group_id            = aws_security_group.service.id
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = local.http_port
  to_port                      = local.http_port
  ip_protocol                  = "tcp"
  description                  = "Interactive-query forwarding between instances"
}

resource "aws_vpc_security_group_egress_rule" "service_to_msk" {
  security_group_id            = aws_security_group.service.id
  referenced_security_group_id = aws_security_group.msk.id
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  description                  = "Kafka"
}

resource "aws_vpc_security_group_egress_rule" "service_to_peers" {
  security_group_id            = aws_security_group.service.id
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = local.http_port
  to_port                      = local.http_port
  ip_protocol                  = "tcp"
  description                  = "Peer queries"
}

resource "aws_vpc_security_group_egress_rule" "service_https" {
  security_group_id = aws_security_group.service.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
  description       = "ECR, Secrets Manager, CloudWatch Logs and STS (via NAT or VPC endpoints)"
}

resource "aws_vpc_security_group_ingress_rule" "alb_from_clients" {
  for_each          = toset(var.allowed_cidr_blocks)
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = each.value
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
  description       = "API clients"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_service_api" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = local.http_port
  to_port                      = local.http_port
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_service_health" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = aws_security_group.service.id
  from_port                    = local.management_port
  to_port                      = local.management_port
  ip_protocol                  = "tcp"
}

# ---------------------------------------------------------------- kafka

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = var.name

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

# ---------------------------------------------------------------- identity

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secret" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.auth_tokens_secret_arn]
  }
}

resource "aws_iam_role_policy" "execution_secret" {
  name   = "read-auth-tokens"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secret.json
}

resource "aws_iam_role" "task" {
  name               = "${var.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# Least privilege for Kafka: this cluster only, the application's own topics and group only.
data "aws_iam_policy_document" "task_kafka" {
  statement {
    sid       = "ConnectToCluster"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:WriteDataIdempotently"]
    resources = [aws_msk_serverless_cluster.this.arn]
  }

  statement {
    sid = "OwnTopics"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
    ]
    resources = [
      "${local.topic_arn_base}/${local.topic_prefix}.*",
      "${local.topic_arn_base}/${local.application_id}-*",
    ]
  }

  statement {
    sid       = "OwnConsumerGroup"
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = ["${local.group_arn_base}/${local.application_id}"]
  }

  statement {
    sid       = "OwnTransactions"
    actions   = ["kafka-cluster:DescribeTransactionalId", "kafka-cluster:AlterTransactionalId"]
    resources = ["${local.txn_arn_base}/*"]
  }
}

resource "aws_iam_role_policy" "task_kafka" {
  name   = "kafka-access"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_kafka.json
}

# ---------------------------------------------------------------- service

resource "aws_cloudwatch_log_group" "service" {
  name              = "/ecs/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "this" {
  name = var.name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "service" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name         = "fraudstream"
      image        = var.container_image
      essential    = true
      portMappings = [{ containerPort = local.http_port }, { containerPort = local.management_port }]
      environment = [
        { name = "FRAUDSTREAM_BOOTSTRAP_SERVERS", value = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam },
        { name = "FRAUDSTREAM_APPLICATION_ID", value = local.application_id },
        { name = "FRAUDSTREAM_ADVERTISED_HOST", value = "auto" },
        { name = "FRAUDSTREAM_REPLICATION_FACTOR", value = "3" },
        { name = "FRAUDSTREAM_PARTITIONS", value = "6" },
        { name = "FRAUDSTREAM_GRACE", value = var.grace },
        { name = "FRAUDSTREAM_SEED_RULES", value = "true" },
        { name = "FRAUDSTREAM_KAFKA_SECURITY_PROTOCOL", value = "SASL_SSL" },
        { name = "FRAUDSTREAM_KAFKA_SASL_MECHANISM", value = "AWS_MSK_IAM" },
        { name = "FRAUDSTREAM_KAFKA_SASL_JAAS_CONFIG", value = "software.amazon.msk.auth.iam.IAMLoginModule required;" },
        { name = "FRAUDSTREAM_KAFKA_SASL_CLIENT_CALLBACK_HANDLER_CLASS", value = "software.amazon.msk.auth.iam.IAMClientCallbackHandler" },
        { name = "FRAUDSTREAM_KAFKA_NUM_STANDBY_REPLICAS", value = "1" },
      ]
      secrets = [
        { name = "FRAUDSTREAM_AUTH_TOKENS", valueFrom = var.auth_tokens_secret_arn },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.service.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "app"
        }
      }
      readonlyRootFilesystem = false
      stopTimeout            = 60
    }
  ])
}

resource "aws_lb" "this" {
  name               = var.name
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.private_subnet_ids
}

resource "aws_lb_target_group" "api" {
  name        = var.name
  port        = local.http_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/readyz"
    port                = tostring(local.management_port)
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
  }

  deregistration_delay = 30
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

resource "aws_ecs_service" "service" {
  name            = var.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.service.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  # Stopping a task triggers a rebalance; replace one at a time.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 150
  health_check_grace_period_seconds  = 120

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "fraudstream"
    container_port   = local.http_port
  }

  depends_on = [aws_lb_listener.http]
}
