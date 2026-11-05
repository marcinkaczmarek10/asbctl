"""Typed views of the JSON that asbctl prints, as described in docs/output-format.md."""

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True, slots=True)
class TypedValue:
    """An AMQP application property that JSON cannot hold, such as a timestamp or a uuid."""

    type: str
    value: str


type PropertyValue = str | int | float | bool | TypedValue


@dataclass(frozen=True, slots=True)
class Message:
    """One peeked message. Fields that asbctl omits when empty are None."""

    schema: int
    body: bytes
    body_type: str = "data"
    body_text: str | None = None
    sequence_number: int | None = None
    message_id: str | None = None
    enqueued_at: datetime | None = None
    content_type: str | None = None
    correlation_id: str | None = None
    subject: str | None = None
    session_id: str | None = None
    partition_key: str | None = None
    to: str | None = None
    reply_to: str | None = None
    time_to_live_seconds: int | None = None
    delivery_count: int | None = None
    dead_letter_reason: str | None = None
    dead_letter_error_description: str | None = None
    dead_letter_source: str | None = None
    application_properties: Mapping[str, PropertyValue] = MappingProxyType({})

    @property
    def payload_type_id(self) -> str | None:
        """The PayloadTypeId application property, when it is a string."""
        value = self.application_properties.get("PayloadTypeId")
        return value if isinstance(value, str) else None


@dataclass(frozen=True, slots=True)
class Entity:
    """Counts and settings of the entity. They are all None when available is False."""

    available: bool
    queue: str | None = None
    topic: str | None = None
    subscription: str | None = None
    active_messages: int | None = None
    dead_letter_messages: int | None = None
    scheduled_messages: int | None = None
    transfer_dead_letter_messages: int | None = None
    max_delivery_count: int | None = None
    lock_duration: str | None = None
    requires_duplicate_detection: bool | None = None
    duplicate_detection_window: str | None = None


@dataclass(frozen=True, slots=True)
class Group:
    """Dead-lettered messages with one payload type and one reason.

    An empty payload_type_id means the messages have no PayloadTypeId property.
    """

    payload_type_id: str
    dead_letter_reason: str
    count: int
    max_delivery_count: int
    first_enqueued_at: datetime | None = None
    last_enqueued_at: datetime | None = None
    top_dead_letter_error_description: str | None = None
    sample_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class Summary:
    """The result of dlq summary."""

    schema: int
    entity: Entity
    peeked: int
    partial: bool
    groups: tuple[Group, ...]


def message_from_record(record: Mapping[str, Any]) -> Message:
    """Build a Message from one record. The body is decoded from base64 and must be valid."""
    properties = record.get("application_properties", {})
    return Message(
        schema=record["schema"],
        body=base64.b64decode(record.get("body", ""), validate=True),
        body_type=record.get("body_type", "data"),
        body_text=record.get("body_text"),
        sequence_number=record.get("sequence_number"),
        message_id=record.get("message_id"),
        enqueued_at=_time_from(record.get("enqueued_at")),
        content_type=record.get("content_type"),
        correlation_id=record.get("correlation_id"),
        subject=record.get("subject"),
        session_id=record.get("session_id"),
        partition_key=record.get("partition_key"),
        to=record.get("to"),
        reply_to=record.get("reply_to"),
        time_to_live_seconds=record.get("time_to_live_seconds"),
        delivery_count=record.get("delivery_count"),
        dead_letter_reason=record.get("dead_letter_reason"),
        dead_letter_error_description=record.get("dead_letter_error_description"),
        dead_letter_source=record.get("dead_letter_source"),
        application_properties=MappingProxyType(
            {name: _property_from(value) for name, value in properties.items()}
        ),
    )


def summary_from_record(record: Mapping[str, Any]) -> Summary:
    """Build a Summary from the one JSON object that dlq summary prints."""
    return Summary(
        schema=record["schema"],
        entity=Entity(**record["entity"]),
        peeked=record["peeked"],
        partial=record["partial"],
        groups=tuple(_group_from(group) for group in record["groups"]),
    )


def _group_from(record: Mapping[str, Any]) -> Group:
    return Group(
        payload_type_id=record["payload_type_id"],
        dead_letter_reason=record["dead_letter_reason"],
        count=record["count"],
        max_delivery_count=record["max_delivery_count"],
        first_enqueued_at=_time_from(record.get("first_enqueued_at")),
        last_enqueued_at=_time_from(record.get("last_enqueued_at")),
        top_dead_letter_error_description=record.get("top_dead_letter_error_description"),
        sample_message_id=record.get("sample_message_id"),
    )


def _property_from(value: Any) -> PropertyValue:
    if isinstance(value, dict):
        return TypedValue(type=value["type"], value=value["value"])
    return value


def _time_from(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


@dataclass(frozen=True, slots=True)
class ExportResult:
    """What export printed when it wrote the records to a file."""

    schema: int
    exported: int
    file: str
    partial: bool


@dataclass(frozen=True, slots=True)
class PayloadTypeCount:
    """How many messages of one payload type a send would send. An empty id means no property."""

    payload_type_id: str
    count: int


@dataclass(frozen=True, slots=True)
class SendPlan:
    """What send would do. It is printed instead of sending when --yes is missing."""

    schema: int
    total: int
    payload_types: tuple[PayloadTypeCount, ...]
    dry_run: bool
    queue: str | None = None
    topic: str | None = None


@dataclass(frozen=True, slots=True)
class SendResult:
    """The outcome of one message. line is the line of the input that held it."""

    schema: int
    line: int
    status: str
    message_id: str | None = None
    error: str | None = None

    @property
    def sent(self) -> bool:
        return self.status == "sent"


def export_result_from_record(record: Mapping[str, Any]) -> ExportResult:
    """Build an ExportResult from the one JSON object that export prints."""
    return ExportResult(
        schema=record["schema"],
        exported=record["exported"],
        file=record["file"],
        partial=record["partial"],
    )


def send_plan_from_record(record: Mapping[str, Any]) -> SendPlan:
    """Build a SendPlan from the one JSON object that send prints without --yes."""
    target = record["target"]
    return SendPlan(
        schema=record["schema"],
        total=record["total"],
        payload_types=tuple(
            PayloadTypeCount(row["payload_type_id"], row["count"])
            for row in record["payload_types"]
        ),
        dry_run=record["dry_run"],
        queue=target.get("queue"),
        topic=target.get("topic"),
    )


def send_result_from_record(record: Mapping[str, Any]) -> SendResult:
    """Build a SendResult from one result line of send."""
    return SendResult(
        schema=record["schema"],
        line=record["line"],
        status=record["status"],
        message_id=record.get("message_id"),
        error=record.get("error"),
    )


def record_from_message(message: Message) -> dict[str, Any]:
    """Build the record that export writes and send reads, the inverse of message_from_record."""
    record: dict[str, Any] = {
        "schema": message.schema,
        "body": base64.b64encode(message.body).decode("ascii"),
    }
    optional = {
        "sequence_number": message.sequence_number,
        "message_id": message.message_id,
        "enqueued_at": None if message.enqueued_at is None else message.enqueued_at.isoformat(),
        "content_type": message.content_type,
        "correlation_id": message.correlation_id,
        "subject": message.subject,
        "session_id": message.session_id,
        "partition_key": message.partition_key,
        "to": message.to,
        "reply_to": message.reply_to,
        "time_to_live_seconds": message.time_to_live_seconds,
        "delivery_count": message.delivery_count,
        "dead_letter_reason": message.dead_letter_reason,
        "dead_letter_error_description": message.dead_letter_error_description,
        "dead_letter_source": message.dead_letter_source,
        "body_text": message.body_text,
    }
    record.update({name: value for name, value in optional.items() if value is not None})
    if message.body_type != "data":
        record["body_type"] = message.body_type
    if message.application_properties:
        record["application_properties"] = {
            name: _json_from_property(value)
            for name, value in message.application_properties.items()
        }
    return record


def _json_from_property(value: PropertyValue) -> Any:
    if isinstance(value, TypedValue):
        return {"type": value.type, "value": value.value}
    return value
