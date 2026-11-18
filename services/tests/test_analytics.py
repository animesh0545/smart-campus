"""Usage Analytics sink: store Flink window results in DynamoDB and serve the summary."""
import importlib.util
import pathlib
from datetime import datetime, timezone

import boto3
from moto import mock_aws

SERVICES = pathlib.Path(__file__).resolve().parents[1]


def test_store_and_summarise(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    with mock_aws():
        from common import aws, config
        aws.table.cache_clear()
        aws.dynamodb.cache_clear()
        config.ANALYTICS_TABLE = "test-analytics"
        boto3.client("dynamodb", region_name="us-east-1").create_table(
            TableName="test-analytics", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "resourceId", "KeyType": "HASH"},
                       {"AttributeName": "windowStart", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "resourceId", "AttributeType": "S"},
                                  {"AttributeName": "windowStart", "AttributeType": "S"}])
        spec = importlib.util.spec_from_file_location("ua", SERVICES / "usage-analytics/app.py")
        ua = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ua)
        now = datetime.now(timezone.utc).isoformat()
        ua.store_result({"resourceId": "ROOM-001", "resourceType": "ROOM", "windowStart": now,
                         "windowEnd": now, "bookings": 3, "cancellations": 1, "conflicts": 2,
                         "occupancyMinutes": 150, "uniqueUsers": 2, "utilisationPct": 4.5})
        ua.store_result({"resourceId": "EQ-001", "resourceType": "EQUIPMENT", "windowStart": now,
                         "windowEnd": now, "bookings": 1, "cancellations": 0, "conflicts": 0,
                         "occupancyMinutes": 60, "uniqueUsers": 1, "utilisationPct": 1.0})
        s = ua.summary(minutes=5)
        assert s["totals"]["bookings"] == 4 and s["totals"]["conflicts"] == 2
        assert s["topRoomsByOccupancy"][0]["resourceId"] == "ROOM-001"
        assert ua.by_resource("ROOM-001")["windows"][0]["utilisationPct"] == 4.5
        aws.table.cache_clear()
        aws.dynamodb.cache_clear()
