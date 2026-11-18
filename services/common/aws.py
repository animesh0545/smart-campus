"""boto3 factories. On EKS credentials come from EKS Pod Identity (the
smart-campus-app service account); locally from DynamoDB Local / env vars."""
import functools

import boto3
from botocore.config import Config

from . import config

_BOTO_CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, max_pool_connections=50)


@functools.lru_cache(maxsize=None)
def dynamodb():
    return boto3.resource(
        "dynamodb", region_name=config.AWS_REGION, endpoint_url=config.DYNAMODB_ENDPOINT, config=_BOTO_CFG
    )


@functools.lru_cache(maxsize=None)
def table(name: str):
    return dynamodb().Table(name)


@functools.lru_cache(maxsize=None)
def sqs():
    return boto3.client("sqs", region_name=config.AWS_REGION, config=_BOTO_CFG)


@functools.lru_cache(maxsize=None)
def s3():
    return boto3.client("s3", region_name=config.AWS_REGION, config=_BOTO_CFG)


def ensure_local_tables() -> None:
    """Create the DynamoDB tables in DynamoDB Local (docker-compose only).
    On AWS the tables are created by Terraform, so this does nothing there."""
    if not config.DYNAMODB_ENDPOINT:
        return
    client = dynamodb().meta.client
    existing = set(client.list_tables().get("TableNames", []))
    specs = [(t, "pk", "sk") for t in config.NODE_TABLES.values()]
    specs += [(config.ANALYTICS_TABLE, "resourceId", "windowStart"),
              (config.NOTIFICATIONS_TABLE, "userId", "sk")]
    for name, hk, rk in specs:
        if name in existing:
            continue
        try:
            client.create_table(
                TableName=name,
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[{"AttributeName": hk, "KeyType": "HASH"}, {"AttributeName": rk, "KeyType": "RANGE"}],
                AttributeDefinitions=[{"AttributeName": hk, "AttributeType": "S"},
                                      {"AttributeName": rk, "AttributeType": "S"}],
            )
        except client.exceptions.ResourceInUseException:
            pass
