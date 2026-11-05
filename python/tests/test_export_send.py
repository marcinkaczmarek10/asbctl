import json
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from asbctl import (
    AsbctlError,
    ExportResult,
    Message,
    PayloadTypeCount,
    SendPlan,
    SendResult,
    TypedValue,
    export,
    export_to_file,
    peek,
    send,
    send_plan,
)
from asbctl.models import message_from_record, record_from_message

from .conftest import FakeFactory


def test_export_parses_the_records_and_maps_its_arguments(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("messages.ndjson"))

    messages = list(
        export(namespace="contoso", queue="orders", dlq=True, max_messages=5, payload_type_id="X")
    )

    assert [message.message_id for message in messages] == ["msg-1", "msg-2", "msg-3", "msg-4"]
    assert fake.arguments() == [
        "export", "--namespace", "contoso", "--queue", "orders", "--dlq", "--max", "5",
        "--payload-type-id", "X", "--output", "json",
    ]  # fmt: skip


def test_export_to_file_returns_the_result(
    fake_asbctl: FakeFactory, contract: Callable[[str], str], tmp_path
):
    fake = fake_asbctl(stdout=contract("export_result.json"))

    result = export_to_file(tmp_path / "out.ndjson", namespace="n", queue="q", force=True)

    assert result == ExportResult(schema=1, exported=4, file="orders.ndjson", partial=True)
    assert fake.arguments() == [
        "export", "--namespace", "n", "--queue", "q", "--file", str(tmp_path / "out.ndjson"),
        "--force", "--output", "json",
    ]  # fmt: skip


def test_export_to_file_raises_when_the_file_exists(fake_asbctl: FakeFactory):
    fake_asbctl(
        stderr='{"schema":1,"error":{"code":"file_exists","message":"file already exists: out"}}\n',
        exit_code=1,
    )

    with pytest.raises(AsbctlError) as raised:
        export_to_file("out", queue="q", namespace="n")

    assert raised.value.code == "file_exists"


def test_send_plan_returns_the_plan_and_passes_no_yes(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("send_plan.json"), read_stdin=True)

    plan = send_plan([{"message_id": "m"}], namespace="n", queue="orders", new_message_id=True)

    assert plan == SendPlan(
        schema=1,
        total=3,
        payload_types=(PayloadTypeCount("OrderPlaced", 2), PayloadTypeCount("", 1)),
        dry_run=False,
        queue="orders",
    )
    assert fake.arguments() == [
        "send", "--namespace", "n", "--queue", "orders", "--new-message-id", "--output", "json",
    ]  # fmt: skip


def test_send_writes_records_to_stdin_and_yields_the_results(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("send_result.ndjson"), read_stdin=True)
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

    results = list(
        send([message, {"message_id": "m-2", "body": "eA=="}], namespace="n", topic="t", rate=50)
    )

    assert results == [
        SendResult(schema=1, line=1, status="sent", message_id="msg-1"),
        SendResult(
            schema=1, line=2, status="failed", message_id="msg-2", error="the message is too large"
        ),
        SendResult(schema=1, line=3, status="sent"),
    ]
    assert results[0].sent
    assert not results[1].sent
    assert fake.arguments() == [
        "send", "--namespace", "n", "--topic", "t", "--rate", "50", "--yes", "--output", "json",
    ]  # fmt: skip
    first, second = (json.loads(line) for line in fake.stdin().splitlines())
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


def test_send_raises_after_the_results_when_some_messages_failed(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(
        stdout=contract("send_result.ndjson"),
        stderr='{"schema":1,"error":{"code":"partial","message":"1 of 3 were not sent"}}\n',
        exit_code=3,
        read_stdin=True,
    )
    results = send([{"body": ""}], namespace="n", queue="q")

    assert [next(results).line for _ in range(3)] == [1, 2, 3]
    with pytest.raises(AsbctlError) as raised:
        next(results)
    assert (raised.value.code, raised.value.exit_code) == ("partial", 3)


def test_a_bad_record_is_a_usage_error_naming_the_line(fake_asbctl: FakeFactory):
    fake_asbctl(
        stderr='{"schema":1,"error":{"code":"usage","message":"line 2: schema is 9, want 1"}}\n',
        exit_code=2,
        read_stdin=True,
    )

    with pytest.raises(AsbctlError) as raised:
        list(send([{"body": ""}, {"schema": 9}], namespace="n", queue="q"))

    assert raised.value.exit_code == 2
    assert "line 2" in raised.value.message


def test_a_message_survives_the_round_trip_through_a_record(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(stdout=contract("messages.ndjson"))

    for original in peek(queue="q"):
        assert message_from_record(record_from_message(original)) == original


def test_send_stops_feeding_stdin_when_the_binary_exits_early(fake_asbctl: FakeFactory):
    fake_asbctl(stderr="boom", exit_code=1)

    with pytest.raises(AsbctlError):
        list(send(({"body": "x" * 1000} for _ in range(5000)), namespace="n", queue="q"))
