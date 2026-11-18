"""Environment-driven configuration.

On EKS the values come from the Terraform-managed ConfigMap `platform-config`
and Secret `platform-secrets`; locally they come from docker-compose.yml.
"""
import json
import os


def env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


SERVICE_NAME = env("SERVICE_NAME", "unknown-service")
HTTP_PORT = int(env("HTTP_PORT", "8000"))
GRPC_PORT = int(env("GRPC_PORT", "50051"))

AWS_REGION = env("AWS_REGION", "us-east-1")
# Set only for local development (DynamoDB Local); empty on AWS.
DYNAMODB_ENDPOINT = env("DYNAMODB_ENDPOINT") or None

# --- Kafka (MSK) ---
KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP")
KAFKA_REPLICATION = int(env("KAFKA_REPLICATION", "2"))
BOOKING_EVENTS_TOPIC = env("BOOKING_EVENTS_TOPIC", "booking-events")
ANALYTICS_TOPIC = env("ANALYTICS_TOPIC", "analytics-results")

# --- SQS / S3 ---
SQS_QUEUE_URL = env("SQS_QUEUE_URL")
UPLOADS_BUCKET = env("UPLOADS_BUCKET")

# --- DynamoDB tables ---
# {"node-0": "smart-campus-bookings-node-0", ...}
NODE_TABLES: dict = json.loads(env("NODE_TABLES", "{}") or "{}")
INITIAL_NODES = [n for n in env("INITIAL_NODES", "node-0,node-1,node-2").split(",") if n]
ANALYTICS_TABLE = env("ANALYTICS_TABLE", "smart-campus-analytics-results")
NOTIFICATIONS_TABLE = env("NOTIFICATIONS_TABLE", "smart-campus-notifications")
VNODES = int(env("VNODES", "150"))

# --- PostgreSQL (RDS) ---
DB_HOST = env("DB_HOST", "localhost")
DB_PORT = int(env("DB_PORT", "5432"))
DB_USER = env("DB_USER", "campusadmin")
DB_PASSWORD = env("DB_PASSWORD", "campus")

# --- ZooKeeper ---
ZK_HOSTS = env("ZK_HOSTS", "localhost:2181")

# --- peer services (K8s DNS names; docker-compose uses the same names) ---
USER_SERVICE_URL = env("USER_SERVICE_URL", "http://user-service:8000")
RESOURCE_REGISTRY_URL = env("RESOURCE_REGISTRY_URL", "http://resource-registry:8000")
# dns:/// + headless Service + round_robin spreads gRPC calls over all pods,
# so HPA scale-out also helps internal gRPC traffic (plain ClusterIP pins one pod).
AVAILABILITY_GRPC = env("AVAILABILITY_GRPC", "dns:///availability-headless:50051")
PARTITION_GRPC = env("PARTITION_GRPC", "dns:///partition-manager-headless:50051")
