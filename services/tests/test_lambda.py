"""Notification Lambda handler tests with stubbed AWS clients."""
import importlib.util
import io
import json
import pathlib

spec = importlib.util.spec_from_file_location(
    "handler", pathlib.Path(__file__).resolve().parents[1] / "notification-lambda" / "handler.py")
handler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(handler)


class FakeTable:
    def __init__(self):
        self.items = []

    def put_item(self, Item, ConditionExpression=None):
        if any(i["sk"] == Item["sk"] for i in self.items):
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items.append(Item)


class FakeS3:
    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(b"id,name,type\nROOM-900,New Room,ROOM\nEQ-900,Cam,EQUIPMENT\n")}


def setup_function(_):
    handler._ddb = FakeTable()
    handler._s3 = FakeS3()
    handler.TOPIC = ""


def test_sqs_booking_created():
    ev = {"eventId": "e1", "type": "CREATED", "bookingId": "b1", "resourceId": "ROOM-001",
          "userId": "U001", "date": "2026-11-18", "startTime": "10:00", "endTime": "11:00"}
    out = handler.lambda_handler({"Records": [{"eventSource": "aws:sqs", "messageId": "m1",
                                               "body": json.dumps(ev)}]}, None)
    assert out == {"batchItemFailures": []}
    item = handler._ddb.items[0]
    assert item["userId"] == "U001" and "confirmed" in item["message"]


def test_bad_sqs_message_reported_as_partial_failure():
    out = handler.lambda_handler({"Records": [{"eventSource": "aws:sqs", "messageId": "m2",
                                               "body": "not json"}]}, None)
    assert out == {"batchItemFailures": [{"itemIdentifier": "m2"}]}


def test_s3_import_notice():
    rec = {"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"}, "object": {"key": "imports/x.csv"}}}
    handler.lambda_handler({"Records": [rec]}, None)
    item = handler._ddb.items[0]
    assert item["userId"] == "admin" and item["rows"] == 2
