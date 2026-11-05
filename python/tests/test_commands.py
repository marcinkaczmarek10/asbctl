from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from asbctl import AsbctlError, Entity, Group, TypedValue, dlq_summary, peek
from asbctl.models import message_from_record

from .conftest import FakeFactory


def test_peek_parses_the_contract_messages(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(stdout=contract("messages.ndjson"))

    first, dead_lettered, value_body, empty_body = peek(namespace="contoso", queue="orders")

    assert first.sequence_number == 1001
    assert first.message_id == "msg-1"
    assert first.enqueued_at == datetime(2026, 11, 5, 10, 0, tzinfo=UTC)
    assert first.body == b'{"id":42}'
    assert first.body_text == '{"id":42}'
    assert first.body_type == "data"
    assert first.time_to_live_seconds == 3600
    assert first.payload_type_id == "OrderPlaced"
    assert first.application_properties == {
        "PayloadTypeId": "OrderPlaced",
        "created": TypedValue("timestamp", "2026-11-05T09:59:00.5Z"),
        "ratio": 1.5,
        "retries": 3,
        "signature": TypedValue("binary", "3q0="),
        "trace": TypedValue("uuid", "01020304-0506-0708-090a-0b0c0d0e0f10"),
        "urgent": True,
    }
    assert dead_lettered.body == b"\xff\xfe\x00"
    assert dead_lettered.body_text is None
    assert dead_lettered.dead_letter_reason == "MaxDeliveryCountExceeded"
    assert dead_lettered.delivery_count == 10
    assert dead_lettered.payload_type_id is None
    assert value_body.body_type == "value"
    assert value_body.body == b""
    assert value_body.body_text == '{"id":7,"tags":["a","b"]}'
    assert empty_body.body == b""
    assert empty_body.body_text == ""
    assert empty_body.application_properties == {}


def test_peek_maps_its_arguments_to_flags(fake_asbctl: FakeFactory):
    fake = fake_asbctl()

    list(
        peek(
            namespace="contoso",
            topic="events",
            subscription="billing",
            dlq=True,
            max_messages=20,
            from_sequence=5,
            payload_type_id="OrderPlaced",
        )
    )

    assert fake.arguments() == [
        "peek", "--namespace", "contoso", "--topic", "events", "--subscription", "billing",
        "--dlq", "--max", "20", "--from-sequence", "5", "--payload-type-id", "OrderPlaced",
        "--output", "json",
    ]  # fmt: skip


def test_peek_decodes_nothing_until_the_message_is_read(fake_asbctl: FakeFactory):
    fake_asbctl(stdout='{"schema":1,"body":"not base64!"}\n')

    messages = peek(queue="q")

    with pytest.raises(ValueError, match="base64"):
        next(messages)


def test_a_body_that_is_not_valid_base64_is_rejected():
    with pytest.raises(ValueError, match="base64"):
        message_from_record({"schema": 1, "body": "a b c"})


def test_peek_raises_the_error_asbctl_reported(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(stderr=contract("error.json"), exit_code=1)

    with pytest.raises(AsbctlError) as raised:
        list(peek(queue="q"))

    assert (raised.value.code, raised.value.message) == ("peek_failed", "service unavailable")


def test_dlq_summary_parses_the_contract_summary(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("summary.json"))

    summary = dlq_summary(namespace="contoso", queue="orders", max_messages=500)

    assert fake.arguments() == [
        "dlq", "summary", "--namespace", "contoso", "--queue", "orders",
        "--max", "500", "--output", "json",
    ]  # fmt: skip
    assert summary.schema == 1
    assert summary.peeked == 4
    assert summary.partial is True
    assert summary.entity == Entity(
        available=True,
        queue="orders",
        active_messages=12,
        dead_letter_messages=40,
        scheduled_messages=0,
        transfer_dead_letter_messages=0,
        max_delivery_count=10,
        lock_duration="PT1M",
        requires_duplicate_detection=True,
        duplicate_detection_window="PT10M",
    )
    assert summary.groups == (
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
    )


def test_dlq_summary_with_unavailable_entity_details(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(stdout=contract("summary_unavailable.json"))

    summary = dlq_summary(topic="events", subscription="billing")

    assert summary.entity == Entity(available=False, topic="events", subscription="billing")
    assert summary.entity.dead_letter_messages is None
    assert summary.partial is True
