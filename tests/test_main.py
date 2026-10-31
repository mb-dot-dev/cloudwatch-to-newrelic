from typing import TYPE_CHECKING

from app.main import lambda_handler
from tests.conftest import payload

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_lambda_powertools.utilities.typing import LambdaContext

    from app.models import FirehoseRecord


def test_handler_processes_each_record_independently(
    make_record: Callable[..., FirehoseRecord],
    lambda_context: LambdaContext,
) -> None:
    records = (
        make_record(payload(), "a"),
        make_record(b"junk", "b"),
        make_record(payload(message_type="CONTROL_MESSAGE"), "c"),
    )
    event = {
        "invocationId": "i",
        "deliveryStreamArn": "arn",
        "region": "eu-central-1",
        "records": [r.model_dump(by_alias=True) for r in records],
    }

    out = lambda_handler(event, lambda_context)

    assert [(r["recordId"], r["result"]) for r in out["records"]] == [
        ("a", "Ok"),
        ("b", "ProcessingFailed"),
        ("c", "Dropped"),
    ]
