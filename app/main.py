from typing import TYPE_CHECKING, Any

from aws_lambda_powertools import Logger

from app.models import FirehoseEvent
from app.transform import transform_record

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    validated = FirehoseEvent.model_validate(event)
    return {"records": [transform_record(record) for record in validated.records]}
