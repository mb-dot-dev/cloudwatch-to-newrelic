import json
import re
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from app.models import LogEvent

ENVIRONMENT: Final = "prod"
LAMBDA_LOG_GROUP_PREFIX: Final = "/aws/lambda/"

type LogEntry = dict[str, Any]

# Powertools keys mapped onto canonical entry attributes instead of passed through
_CONSUMED_KEYS: Final = frozenset({"message", "level", "function_request_id", "function_name", "timestamp"})
_REQUEST_ID_PATTERN: Final = re.compile(r"RequestId: ([0-9a-f-]{36})")


def function_name_from_log_group(log_group: str) -> str:
    return log_group.removeprefix(LAMBDA_LOG_GROUP_PREFIX)


def _parse_json_object(message: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(message)
    # ValueError covers JSONDecodeError and int literals over sys.get_int_max_str_digits()
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _from_powertools(line: dict[str, Any], raw: str, log_group: str) -> LogEntry:
    entry: LogEntry = {key: value for key, value in line.items() if key not in _CONSUMED_KEYS}
    if "message" not in line:
        entry["message"] = raw.rstrip("\n")
    else:
        message = line["message"]
        entry["message"] = message if isinstance(message, str) else json.dumps(message)
    entry["functionName"] = line.get("function_name") or function_name_from_log_group(log_group)
    if level := line.get("level"):
        entry["level"] = level
    if request_id := line.get("function_request_id"):
        entry["requestId"] = request_id
    return entry


def _from_plain_line(message: str, log_group: str) -> LogEntry:
    entry: LogEntry = {
        "message": message.rstrip("\n"),
        "functionName": function_name_from_log_group(log_group),
    }
    if match := _REQUEST_ID_PATTERN.search(message):
        entry["requestId"] = match.group(1)
    return entry


def build_entry(event: LogEvent, log_group: str, log_stream: str) -> LogEntry:
    line = _parse_json_object(event.message)
    entry = (
        _from_powertools(line, event.message, log_group)
        if line is not None
        else _from_plain_line(event.message, log_group)
    )
    entry["timestamp"] = event.timestamp
    entry["environment"] = ENVIRONMENT
    entry["logGroup"] = log_group
    entry["logStream"] = log_stream
    return entry
