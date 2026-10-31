import base64
import binascii
import gzip
import json
from typing import Final
import zlib

from aws_lambda_powertools import Logger
from pydantic import ValidationError

from app.entries import LAMBDA_LOG_GROUP_PREFIX, LogEntry, build_entry
from app.models import CloudWatchPayload, FirehoseRecord

GZIP_MAGIC: Final = b"\x1f\x8b"
CONTROL_MESSAGE: Final = "CONTROL_MESSAGE"

logger = Logger()


def encode_entries(entries: list[LogEntry], payload: CloudWatchPayload) -> bytes:
    """Re-wrap entries in a gzipped CloudWatch-shaped payload, which New Relic splits into one log per event."""
    envelope = {
        "messageType": payload.message_type,
        "owner": payload.owner,
        "logGroup": payload.log_group,
        "logStream": payload.log_stream,
        "subscriptionFilters": payload.subscription_filters,
        "logEvents": [
            {"id": event.id, "timestamp": entry["timestamp"], "message": json.dumps(entry)}
            for event, entry in zip(payload.log_events, entries, strict=True)
        ],
    }
    return gzip.compress(json.dumps(envelope).encode())


def _decode_payload(data: str) -> CloudWatchPayload:
    raw = base64.b64decode(data, validate=True)
    # CloudWatch Logs subscription payloads arrive gzipped (Firehose can't decompress for HTTP endpoints)
    if raw.startswith(GZIP_MAGIC):
        raw = gzip.decompress(raw)
    return CloudWatchPayload.model_validate_json(raw)


def _result(record: FirehoseRecord, result: str, data: str | None = None) -> dict[str, str]:
    return {"recordId": record.record_id, "result": result, "data": data if data is not None else record.data}


def transform_record(record: FirehoseRecord) -> dict[str, str]:
    try:
        payload = _decode_payload(record.data)
    except binascii.Error, gzip.BadGzipFile, EOFError, zlib.error, UnicodeDecodeError, ValidationError:
        logger.exception("Failed to decode record", extra={"recordId": record.record_id})
        return _result(record, "ProcessingFailed")

    if (
        payload.message_type == CONTROL_MESSAGE
        or not payload.log_group.startswith(LAMBDA_LOG_GROUP_PREFIX)
        or not payload.log_events
    ):
        return _result(record, "Dropped")

    # One bad record must never fail the whole Firehose batch
    try:
        entries = [build_entry(event, payload.log_group, payload.log_stream) for event in payload.log_events]
        data = base64.b64encode(encode_entries(entries, payload)).decode()
    except Exception:
        logger.exception("Failed to transform record", extra={"recordId": record.record_id})
        return _result(record, "ProcessingFailed")
    return _result(record, "Ok", data)
