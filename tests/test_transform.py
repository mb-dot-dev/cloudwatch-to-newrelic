import base64
import gzip
import json
from typing import TYPE_CHECKING, Any

import pytest

from app.transform import transform_record
from tests.conftest import REQUEST_ID, payload

if TYPE_CHECKING:
    from collections.abc import Callable

    from app.models import FirehoseRecord


def decoded_payload(result: dict[str, str]) -> dict[str, Any]:
    return json.loads(gzip.decompress(base64.b64decode(result["data"])))


def decoded_entries(result: dict[str, str]) -> list[dict[str, Any]]:
    return [json.loads(event["message"]) for event in decoded_payload(result)["logEvents"]]


@pytest.mark.parametrize("gzipped", [False, True])
def test_ok_record_has_one_entry_per_event(
    make_record: Callable[..., FirehoseRecord],
    gzipped: bool,  # noqa: FBT001
) -> None:
    out = transform_record(make_record(payload(), gzipped=gzipped))

    assert out["recordId"] == "r1"
    assert out["result"] == "Ok"
    entries = decoded_entries(out)
    assert [e["message"] for e in entries] == ["a", f"START RequestId: {REQUEST_ID}"]
    assert all(e["environment"] == "prod" and e["functionName"] == "helsinki" for e in entries)


def test_ok_record_keeps_cloudwatch_envelope(make_record: Callable[..., FirehoseRecord]) -> None:
    out = decoded_payload(transform_record(make_record(payload())))

    assert {k: v for k, v in out.items() if k != "logEvents"} == {
        "messageType": "DATA_MESSAGE",
        "owner": "233927217152",
        "logGroup": "/aws/lambda/helsinki",
        "logStream": "s",
        "subscriptionFilters": ["f"],
    }
    assert [(e["id"], e["timestamp"]) for e in out["logEvents"]] == [("1", 1), ("2", 2)]


def test_control_message_dropped(make_record: Callable[..., FirehoseRecord]) -> None:
    rec = make_record(payload(message_type="CONTROL_MESSAGE"))

    assert transform_record(rec) == {"recordId": "r1", "result": "Dropped", "data": rec.data}


def test_non_lambda_log_group_dropped(make_record: Callable[..., FirehoseRecord]) -> None:
    assert transform_record(make_record(payload(log_group="/aws/rds/x")))["result"] == "Dropped"


def test_empty_log_events_dropped(make_record: Callable[..., FirehoseRecord]) -> None:
    assert transform_record(make_record(payload(events=[])))["result"] == "Dropped"


@pytest.mark.parametrize("raw", [b"not json", b'{"messageType": "DATA_MESSAGE"}', b"\x1f\x8bbroken"])
def test_corrupt_record_fails_alone(make_record: Callable[..., FirehoseRecord], raw: bytes) -> None:
    rec = make_record(raw)

    assert transform_record(rec) == {"recordId": "r1", "result": "ProcessingFailed", "data": rec.data}


def test_invalid_base64_fails(make_record: Callable[..., FirehoseRecord]) -> None:
    rec = make_record(payload()).model_copy(update={"data": "not base64!"})

    assert transform_record(rec)["result"] == "ProcessingFailed"
