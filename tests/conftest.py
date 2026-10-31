import base64
import gzip
import json
from typing import TYPE_CHECKING, Any, cast

import pytest

from app.models import FirehoseRecord

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_lambda_powertools.utilities.typing import LambdaContext as PowertoolsLambdaContext

REQUEST_ID = "8f507cfc-1234-4abc-9def-0123456789ab"


def payload(
    log_group: str = "/aws/lambda/helsinki",
    events: list[dict[str, Any]] | None = None,
    message_type: str = "DATA_MESSAGE",
) -> dict[str, Any]:
    return {
        "messageType": message_type,
        "owner": "233927217152",
        "logGroup": log_group,
        "logStream": "s",
        "subscriptionFilters": ["f"],
        "logEvents": events
        if events is not None
        else [
            {"id": "1", "timestamp": 1, "message": '{"level":"INFO","message":"a"}'},
            {"id": "2", "timestamp": 2, "message": f"START RequestId: {REQUEST_ID}"},
        ],
    }


class LambdaContext:
    function_name = "test-function"
    memory_limit_in_mb = 128
    invoked_function_arn = "arn:aws:lambda:eu-central-1:123456789012:function:test-function"
    aws_request_id = "test-request-id"


@pytest.fixture
def lambda_context() -> PowertoolsLambdaContext:
    return cast("PowertoolsLambdaContext", LambdaContext())


@pytest.fixture
def make_record() -> Callable[..., FirehoseRecord]:
    def _make_record(
        data: dict[str, Any] | bytes,
        record_id: str = "r1",
        *,
        gzipped: bool = False,
    ) -> FirehoseRecord:
        raw = json.dumps(data).encode() if isinstance(data, dict) else data
        if gzipped:
            raw = gzip.compress(raw)
        return FirehoseRecord(record_id=record_id, data=base64.b64encode(raw).decode())

    return _make_record
