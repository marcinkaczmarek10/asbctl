import json
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from asbctl.commands import dlq_summary, export, export_to_file, peek, send, send_plan
from asbctl.exceptions import AsbctlError
from asbctl.models import (
    Entity,
    ExportResult,
    Group,
    Message,
    PayloadTypeCount,
    SendPlan,
    SendResult,
    TypedValue,
)

from .conftest import FakeFactory


async def test_peek_yields_the_messages_of_the_contract_file(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake_asbctl(stdout=read_contract("messages.ndjson"))

    # When
    first, dead_lettered, value_body, empty_body = [
        message async for message in peek(namespace="contoso-test", queue="orders-q")
    ]

    # Then
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


async def test_peek_maps_its_arguments_to_flags(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl()

    # When
    [
        message
        async for message in peek(
            namespace="contoso-test",
            topic="events-t",
            subscription="billing-s",
            is_dead_letter=True,
            max_messages=7,
            from_sequence=5,
            payload_type_id="InvoiceSent",
        )
    ]

    # Then
    assert fake.read_arguments() == [
        "peek", "--namespace", "contoso-test", "--topic", "events-t", "--subscription", "billing-s",
        "--dlq", "--max", "7", "--from-sequence", "5", "--payload-type-id", "InvoiceSent",
        "--output", "json",
    ]  # fmt: skip


async def test_peek_rejects_a_body_that_is_not_valid_base64_when_the_message_is_read(
    fake_asbctl: FakeFactory,
):
    # Given
    fake_asbctl(stdout='{"schema":1,"body":"not base64!"}\n')

    # When
    with pytest.raises(ValueError, match="base64"):
        [message async for message in peek(queue="orders-q")]

    # Then


async def test_peek_raises_the_error_asbctl_reported(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake_asbctl(stderr=read_contract("error.json"), exit_code=1)

    # When
    with pytest.raises(AsbctlError) as raised:
        [message async for message in peek(queue="orders-q")]

    # Then
    assert (raised.value.code, raised.value.message) == ("peek_failed", "service unavailable")


async def test_dlq_summary_parses_the_contract_summary(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("summary.json"))

    # When
    summary = await dlq_summary(namespace="contoso-test", queue="orders-q", max_messages=500)

    # Then
    assert fake.read_arguments() == [
        "dlq", "summary", "--namespace", "contoso-test", "--queue", "orders-q",
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


async def test_dlq_summary_keeps_the_entity_unavailable_when_the_service_hid_its_details(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake_asbctl(stdout=read_contract("summary_unavailable.json"))

    # When
    summary = await dlq_summary(topic="events-t", subscription="billing-s")

    # Then
    assert summary.entity == Entity(available=False, topic="events", subscription="billing")
    assert summary.entity.dead_letter_messages is None
    assert summary.partial is True


async def test_export_yields_the_records_and_maps_its_arguments(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("messages.ndjson"))

    # When
    messages = [
        message
        async for message in export(
            namespace="contoso-test",
            queue="orders-q",
            is_dead_letter=True,
            max_messages=5,
            payload_type_id="InvoiceSent",
        )
    ]

    # Then
    assert [message.message_id for message in messages] == ["msg-1", "msg-2", "msg-3", "msg-4"]
    assert fake.read_arguments() == [
        "export", "--namespace", "contoso-test", "--queue", "orders-q", "--dlq", "--max", "5",
        "--payload-type-id", "InvoiceSent", "--output", "json",
    ]  # fmt: skip


async def test_export_to_file_returns_the_result_and_maps_its_arguments(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str], tmp_path
):
    # Given
    fake = fake_asbctl(stdout=read_contract("export_result.json"))
    file_path = tmp_path / "out.ndjson"

    # When
    result = await export_to_file(file_path, namespace="n-test", queue="q-test", can_overwrite=True)

    # Then
    assert result == ExportResult(schema=1, exported=4, file="orders.ndjson", partial=True)
    assert fake.read_arguments() == [
        "export", "--namespace", "n-test", "--queue", "q-test", "--file", str(file_path),
        "--force", "--output", "json",
    ]  # fmt: skip


async def test_export_to_file_raises_when_the_file_exists(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(
        stderr='{"schema":1,"error":{"code":"file_exists","message":"file already exists: out"}}\n',
        exit_code=1,
    )

    # When
    with pytest.raises(AsbctlError) as raised:
        await export_to_file("out", queue="q-test", namespace="n-test")

    # Then
    assert raised.value.code == "file_exists"


async def test_send_plan_returns_the_plan_and_passes_no_yes(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("send_plan.json"), is_stdin_read=True)

    # When
    plan = await send_plan(
        [{"message_id": "m"}], namespace="n-test", queue="orders-q", is_message_id_replaced=True
    )

    # Then
    assert plan == SendPlan(
        schema=1,
        total=3,
        payload_types=(PayloadTypeCount("OrderPlaced", 2), PayloadTypeCount("", 1)),
        dry_run=False,
        queue="orders",
    )
    assert fake.read_arguments() == [
        "send", "--namespace", "n-test", "--queue", "orders-q", "--new-message-id",
        "--output", "json",
    ]  # fmt: skip
    assert fake.read_stdin() == '{"schema":1,"message_id":"m"}\n'


async def test_send_writes_the_records_to_stdin_and_yields_the_results(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("send_result.ndjson"), is_stdin_read=True)
    message = Message(
        schema=1,
        body=b"\xff\x00",
        message_id="m-1",
        enqueued_at=datetime(2026, 11, 5, 10, 0, tzinfo=UTC),
        application_properties={
            "PayloadTypeId": "T",
            "t": TypedValue("timestamp", "2026-11-05T09:59:00Z"),
        },
    )

    # When
    results = [
        result
        async for result in send(
            [message, {"message_id": "m-2", "body": "eA=="}],
            namespace="n-test",
            topic="t-test",
            rate_per_second=13,
        )
    ]

    # Then
    assert results == [
        SendResult(schema=1, line=1, status="sent", message_id="msg-1"),
        SendResult(
            schema=1, line=2, status="failed", message_id="msg-2", error="the message is too large"
        ),
        SendResult(schema=1, line=3, status="sent"),
    ]
    assert results[0].sent
    assert not results[1].sent
    assert fake.read_arguments() == [
        "send", "--namespace", "n-test", "--topic", "t-test", "--rate", "13", "--yes",
        "--output", "json",
    ]  # fmt: skip
    first, second = (json.loads(line) for line in fake.read_stdin().splitlines())
    assert first == {
        "schema": 1,
        "body": "/wA=",
        "message_id": "m-1",
        "enqueued_at": "2026-11-05T10:00:00+00:00",
        "application_properties": {
            "PayloadTypeId": "T",
            "t": {"type": "timestamp", "value": "2026-11-05T09:59:00Z"},
        },
    }
    assert second == {"schema": 1, "message_id": "m-2", "body": "eA=="}


async def test_send_accepts_an_async_iterable_of_records(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl(is_stdin_read=True)

    async def records():
        yield {"message_id": "a-1"}
        yield {"message_id": "a-2"}

    # When
    [result async for result in send(records(), namespace="n-test", queue="q-test")]

    # Then
    assert fake.read_stdin().splitlines() == [
        '{"schema":1,"message_id":"a-1"}',
        '{"schema":1,"message_id":"a-2"}',
    ]


async def test_send_raises_after_the_results_when_some_messages_failed(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake_asbctl(
        stdout=read_contract("send_result.ndjson"),
        stderr='{"schema":1,"error":{"code":"partial","message":"1 of 3 were not sent"}}\n',
        exit_code=3,
        is_stdin_read=True,
    )
    results = send([{"body": ""}], namespace="n-test", queue="q-test")

    # When
    lines = [(await anext(results)).line for _ in range(3)]
    with pytest.raises(AsbctlError) as raised:
        await anext(results)

    # Then
    assert lines == [1, 2, 3]
    assert (raised.value.code, raised.value.exit_code) == ("partial", 3)


async def test_send_raises_a_usage_error_that_names_the_bad_line(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(
        stderr='{"schema":1,"error":{"code":"usage","message":"line 2: schema is 9, want 1"}}\n',
        exit_code=2,
        is_stdin_read=True,
    )

    # When
    with pytest.raises(AsbctlError) as raised:
        [result async for result in send([{"body": ""}, {"schema": 9}], namespace="n", queue="q")]

    # Then
    assert raised.value.exit_code == 2
    assert "line 2" in raised.value.message
