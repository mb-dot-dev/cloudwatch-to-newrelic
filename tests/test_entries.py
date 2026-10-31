import json

import pytest

from app.entries import build_entry, function_name_from_log_group
from app.models import LogEvent

LOG_GROUP = "/aws/lambda/helsinki"
STREAM = "2026/10/30/[$LATEST]abc"
TS = 1_761_800_000_000
REQUEST_ID = "8f507cfc-1234-4abc-9def-0123456789ab"


def ev(message: str, ts: int = TS) -> LogEvent:
    return LogEvent(id="1", timestamp=ts, message=message)


def test_function_name_from_log_group() -> None:
    assert function_name_from_log_group("/aws/lambda/helsinki") == "helsinki"


def test_powertools_line_maps_canonical_fields() -> None:
    line = json.dumps(
        {
            "level": "INFO",
            "message": "hi",
            "function_name": "helsinki",
            "function_request_id": "req-1",
            "service": "helsinki",
            "cold_start": True,
            "timestamp": "2026-10-30 10:00:00,000+0000",
        }
    )

    assert build_entry(ev(line), LOG_GROUP, STREAM) == {
        "message": "hi",
        "level": "INFO",
        "requestId": "req-1",
        "functionName": "helsinki",
        "service": "helsinki",
        "cold_start": True,
        "timestamp": TS,
        "environment": "prod",
        "logGroup": LOG_GROUP,
        "logStream": STREAM,
    }


def test_powertools_without_function_name_uses_log_group() -> None:
    entry = build_entry(ev(json.dumps({"level": "ERROR", "message": "x"})), LOG_GROUP, STREAM)

    assert entry["functionName"] == "helsinki"
    assert "requestId" not in entry


def test_powertools_dict_message_is_stringified() -> None:
    entry = build_entry(ev(json.dumps({"level": "INFO", "message": {"k": 1}})), LOG_GROUP, STREAM)

    assert entry["message"] == '{"k": 1}'


def test_canonical_keys_win_over_passthrough() -> None:
    line = json.dumps({"level": "INFO", "message": "m", "environment": "staging", "logGroup": "other"})

    entry = build_entry(ev(line), LOG_GROUP, STREAM)

    assert entry["environment"] == "prod"
    assert entry["logGroup"] == LOG_GROUP
    assert entry["timestamp"] == TS


def test_platform_line_parses_request_id() -> None:
    line = f"END RequestId: {REQUEST_ID}\n"

    assert build_entry(ev(line), LOG_GROUP, STREAM) == {
        "message": f"END RequestId: {REQUEST_ID}",
        "requestId": REQUEST_ID,
        "functionName": "helsinki",
        "timestamp": TS,
        "environment": "prod",
        "logGroup": LOG_GROUP,
        "logStream": STREAM,
    }


def test_plain_line_without_request_id() -> None:
    entry = build_entry(ev("INIT_START Runtime Version: python:3.15\n"), LOG_GROUP, STREAM)

    assert "requestId" not in entry
    assert "level" not in entry
    assert entry["message"] == "INIT_START Runtime Version: python:3.15"


@pytest.mark.parametrize("line", ['"hello"', "42", "[1, 2]", "{not json"])
def test_json_non_object_is_plain_line(line: str) -> None:
    assert build_entry(ev(line), LOG_GROUP, STREAM)["message"] == line
