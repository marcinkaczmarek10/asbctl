import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone

import pytest

from asbctl.models import (
    Entity,
    ExportResult,
    Group,
    Message,
    PayloadTypeCount,
    SendPlan,
    SendResult,
    Summary,
    TypedValue,
    export_result_from_record,
    message_from_record,
    record_from_message,
    send_plan_from_record,
    send_result_from_record,
    summary_from_record,
)


def test_message_from_record_reads_only_the_required_fields_when_the_rest_is_omitted():
    # Given
    record = {"schema": 1, "body": ""}

    # When
    message = message_from_record(record)

    # Then
    assert message == Message(schema=1, body=b"")
    assert message.body_type == "data"
    assert message.application_properties == {}
    assert message.payload_type_id is None


def test_message_from_record_reads_every_field():
    # Given
    record = {
        "schema": 1,
        "sequence_number": 77,
        "message_id": "id-77",
        "enqueued_at": "2026-11-05T10:00:00.5Z",
        "content_type": "text/plain",
        "correlation_id": "corr-77",
        "subject": "subject-77",
        "session_id": "session-77",
        "partition_key": "partition-77",
        "to": "to-77",
        "reply_to": "reply-77",
        "time_to_live_seconds": 77,
        "delivery_count": 7,
        "dead_letter_reason": "reason-77",
        "dead_letter_error_description": "description-77",
        "dead_letter_source": "source-77",
        "body_type": "value",
        "body": "AQID",
        "body_text": "text-77",
        "application_properties": {
            "PayloadTypeId": "Invoice",
            "when": {"type": "timestamp", "value": "2026-11-05T09:00:00Z"},
        },
    }

    # When
    message = message_from_record(record)

    # Then
    assert message == Message(
        schema=1,
        body=b"\x01\x02\x03",
        body_type="value",
        body_text="text-77",
        sequence_number=77,
        message_id="id-77",
        enqueued_at=datetime(2026, 11, 5, 10, 0, 0, 500000, tzinfo=UTC),
        content_type="text/plain",
        correlation_id="corr-77",
        subject="subject-77",
        session_id="session-77",
        partition_key="partition-77",
        to_address="to-77",
        reply_to_address="reply-77",
        time_to_live_seconds=77,
        delivery_count=7,
        dead_letter_reason="reason-77",
        dead_letter_error_description="description-77",
        dead_letter_source="source-77",
        application_properties={
            "PayloadTypeId": "Invoice",
            "when": TypedValue("timestamp", "2026-11-05T09:00:00Z"),
        },
    )
    assert message.payload_type_id == "Invoice"


@pytest.mark.parametrize("body", ["a b c", "not base64!", "AQI", "QQ==!"])
def test_message_from_record_rejects_a_body_that_is_not_valid_base64(body: str):
    # Given
    record = {"schema": 1, "body": body}

    # When
    with pytest.raises(ValueError, match=r"base64|padding"):
        message_from_record(record)

    # Then


def test_message_payload_type_id_ignores_a_property_that_is_not_a_string():
    # Given
    message = Message(schema=1, body=b"", application_properties={"PayloadTypeId": 5})

    # When
    payload_type_id = message.payload_type_id

    # Then
    assert payload_type_id is None


def test_message_cannot_be_changed_after_it_is_built():
    # Given
    message = Message(schema=1, body=b"")

    # When
    with pytest.raises(AttributeError):
        message.schema = 2  # ty: ignore[invalid-assignment]

    # Then


def test_record_from_message_writes_only_the_fields_that_are_set():
    # Given
    message = Message(schema=1, body=b"\xff\x00")

    # When
    record = record_from_message(message)

    # Then
    assert record == {"schema": 1, "body": "/wA="}


def test_record_from_message_writes_every_field_under_its_record_name():
    # Given
    message = Message(
        schema=1,
        body=b"x",
        body_type="sequence",
        body_text="text-5",
        sequence_number=5,
        message_id="id-5",
        enqueued_at=datetime(2026, 11, 5, 10, 0, tzinfo=UTC),
        content_type="text/plain",
        correlation_id="corr-5",
        subject="subject-5",
        session_id="session-5",
        partition_key="partition-5",
        to_address="to-5",
        reply_to_address="reply-5",
        time_to_live_seconds=5,
        delivery_count=5,
        dead_letter_reason="reason-5",
        dead_letter_error_description="description-5",
        dead_letter_source="source-5",
        application_properties={"kept": 1, "when": TypedValue("timestamp", "2026-11-05T09:00:00Z")},
    )

    # When
    record = record_from_message(message)

    # Then
    assert record == {
        "schema": 1,
        "body": "eA==",
        "body_type": "sequence",
        "body_text": "text-5",
        "sequence_number": 5,
        "message_id": "id-5",
        "enqueued_at": "2026-11-05T10:00:00Z",
        "content_type": "text/plain",
        "correlation_id": "corr-5",
        "subject": "subject-5",
        "session_id": "session-5",
        "partition_key": "partition-5",
        "to": "to-5",
        "reply_to": "reply-5",
        "time_to_live_seconds": 5,
        "delivery_count": 5,
        "dead_letter_reason": "reason-5",
        "dead_letter_error_description": "description-5",
        "dead_letter_source": "source-5",
        "application_properties": {
            "kept": 1,
            "when": {"type": "timestamp", "value": "2026-11-05T09:00:00Z"},
        },
    }


def test_record_from_message_keeps_an_empty_body_text_and_a_zero_count():
    # Given
    message = Message(schema=1, body=b"", body_text="", delivery_count=0)

    # When
    record = record_from_message(message)

    # Then
    assert record == {"schema": 1, "body": "", "body_text": "", "delivery_count": 0}


def test_record_from_message_writes_a_time_that_is_not_utc_as_the_same_instant_in_utc():
    # Given
    message = Message(
        schema=1,
        body=b"",
        enqueued_at=datetime(2026, 11, 5, 12, 0, tzinfo=timezone(timedelta(hours=2))),
    )

    # When
    record = record_from_message(message)

    # Then
    assert record["enqueued_at"] == "2026-11-05T10:00:00Z"


def test_message_survives_a_round_trip_through_a_record(read_contract: Callable[[str], str]):
    # Given
    records = [json.loads(line) for line in read_contract("messages.ndjson").splitlines()]

    # When
    round_tripped = [
        message_from_record(record_from_message(message_from_record(r))) for r in records
    ]

    # Then
    assert round_tripped == [message_from_record(record) for record in records]


def test_summary_from_record_reads_the_entity_and_the_groups(read_contract: Callable[[str], str]):
    # Given
    record = json.loads(read_contract("summary.json"))

    # When
    summary = summary_from_record(record)

    # Then
    assert summary == Summary(
        schema=1,
        entity=Entity(
            is_available=True,
            queue="orders",
            active_messages=12,
            dead_letter_messages=40,
            scheduled_messages=0,
            transfer_dead_letter_messages=0,
            max_delivery_count=10,
            lock_duration_iso8601="PT1M",
            is_duplicate_detection_required=True,
            duplicate_detection_window_iso8601="PT10M",
        ),
        peeked=4,
        is_partial=True,
        groups=(
            Group(
                payload_type_id="OrderPlaced",
                dead_letter_reason="MaxDeliveryCountExceeded",
                count=3,
                max_delivery_count=10,
                first_enqueued_at=datetime(2026, 11, 5, 10, 0, tzinfo=UTC),
                last_enqueued_at=datetime(2026, 11, 5, 10, 10, tzinfo=UTC),
                top_dead_letter_error_description="consumer timed out",
                sample_message_id="a1",
            ),
            Group(
                payload_type_id="",
                dead_letter_reason="BadPayload",
                count=1,
                max_delivery_count=1,
                first_enqueued_at=datetime(2026, 11, 5, 11, 0, tzinfo=UTC),
                last_enqueued_at=datetime(2026, 11, 5, 11, 0, tzinfo=UTC),
                top_dead_letter_error_description="unexpected token",
                sample_message_id="b1",
            ),
        ),
    )


def test_summary_from_record_leaves_the_entity_details_empty_when_they_are_unavailable():
    # Given
    record = {
        "schema": 1,
        "entity": {"topic": "events", "subscription": "billing", "available": False},
        "peeked": 0,
        "partial": False,
        "groups": [],
    }

    # When
    summary = summary_from_record(record)

    # Then
    assert summary.entity == Entity(is_available=False, topic="events", subscription="billing")
    assert summary.groups == ()
    assert summary.is_partial is False


def test_export_result_from_record_reads_the_object_export_prints():
    # Given
    record = {"schema": 1, "exported": 9, "file": "out-9.ndjson", "partial": False}

    # When
    result = export_result_from_record(record)

    # Then
    assert result == ExportResult(schema=1, exported=9, file_path="out-9.ndjson", is_partial=False)


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ({"queue": "orders-q"}, {"queue": "orders-q", "topic": None}),
        ({"topic": "events-t"}, {"queue": None, "topic": "events-t"}),
    ],
    ids=["queue", "topic"],
)
def test_send_plan_from_record_reads_the_target(
    target: dict[str, str], expected: dict[str, str | None]
):
    # Given
    record = {
        "schema": 1,
        "target": target,
        "total": 5,
        "payload_types": [
            {"payload_type_id": "Invoice", "count": 3},
            {"payload_type_id": "", "count": 2},
        ],
        "dry_run": True,
    }

    # When
    plan = send_plan_from_record(record)

    # Then
    assert plan == SendPlan(
        schema=1,
        total=5,
        payload_types=(PayloadTypeCount("Invoice", 3), PayloadTypeCount("", 2)),
        is_dry_run=True,
        **expected,
    )


@pytest.mark.parametrize(
    ("status", "is_sent"), [("sent", True), ("failed", False)], ids=["sent", "failed"]
)
def test_send_result_from_record_reads_the_line_and_derives_is_sent(status: str, is_sent: bool):
    # Given
    record = {"schema": 1, "line": 6, "message_id": "m-6", "status": status, "error": "e-6"}

    # When
    result = send_result_from_record(record)

    # Then
    assert result == SendResult(schema=1, line=6, status=status, message_id="m-6", error="e-6")
    assert result.is_sent is is_sent


def test_send_result_from_record_leaves_the_optional_fields_empty():
    # Given
    record = {"schema": 1, "line": 3, "status": "sent"}

    # When
    result = send_result_from_record(record)

    # Then
    assert (result.message_id, result.error) == (None, None)


def test_message_from_record_reads_a_missing_body_as_empty_bytes():
    # Given
    record = {"schema": 1}

    # When
    message = message_from_record(record)

    # Then
    assert message.body == b""
