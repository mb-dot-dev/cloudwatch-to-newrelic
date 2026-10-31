from pydantic import BaseModel, ConfigDict, Field


class LogEvent(BaseModel):
    id: str
    timestamp: int
    message: str


class CloudWatchPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    message_type: str = Field(alias="messageType")
    owner: str
    log_group: str = Field(alias="logGroup")
    log_stream: str = Field(alias="logStream")
    subscription_filters: list[str] = Field(alias="subscriptionFilters")
    log_events: list[LogEvent] = Field(alias="logEvents")


class FirehoseRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    record_id: str = Field(alias="recordId")
    data: str


class FirehoseEvent(BaseModel):
    records: list[FirehoseRecord]
