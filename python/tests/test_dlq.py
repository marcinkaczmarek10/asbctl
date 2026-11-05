from collections.abc import Callable

import pytest

from asbctl.dlq import (
    Dedupe,
    DlqGroup,
    DlqPlan,
    DlqResult,
    Location,
    PurgeSummary,
    ResendSummary,
    dlq_plan_from_record,
    dlq_purge,
    dlq_purge_plan,
    dlq_resend,
    dlq_resend_plan,
    dlq_result_from_record,
)
from asbctl.exceptions import AsbctlError

from .conftest import FakeFactory


async def test_dlq_resend_plan_parses_the_contract_plan_and_passes_no_yes(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("dlq_resend_plan.json"))

    # When
    plan = await dlq_resend_plan(
        namespace="n-test",
        queue="orders-q",
        payload_type_id="InvoiceSent",
        reason="Expired",
        max_messages=11,
        is_message_id_replaced=True,
        is_dry_run=True,
    )

    # Then
    assert plan == DlqPlan(
        schema=1,
        command="dlq resend",
        source=Location(queue="orders"),
        target=Location(queue="orders"),
        selected=4,
        groups=(DlqGroup("Order", "Poison", 3), DlqGroup("", "Expired", 1)),
        snapshot_path="asbctl-snapshot-orders-20261105T143005Z.ndjson",
        dedupe=Dedupe(
            is_enabled=True,
            window_seconds=600,
            is_assumed=False,
            repeated_in_selection=2,
            in_ledger=1,
        ),
        warnings=(),
        is_dry_run=False,
    )
    assert fake.read_arguments() == [
        "dlq", "resend", "--namespace", "n-test", "--queue", "orders-q",
        "--payload-type-id", "InvoiceSent", "--reason", "Expired", "--max", "11",
        "--new-message-id", "--dry-run", "--output", "json",
    ]  # fmt: skip


async def test_dlq_purge_plan_parses_a_plan_without_target_or_dedupe(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake_asbctl(stdout=read_contract("dlq_purge_plan.json"))

    # When
    plan = await dlq_purge_plan(
        namespace="n-test", topic="events-t", subscription="billing-s", is_dry_run=True
    )

    # Then
    assert plan.command == "dlq purge"
    assert plan.source == Location(topic="events", subscription="billing")
    assert plan.target is None
    assert plan.dedupe is None
    assert plan.is_dry_run is True
    assert plan.selected == 4


async def test_dlq_resend_yields_each_result_then_the_summary(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("dlq_resend_result.ndjson"))

    # When
    events = [
        event
        async for event in dlq_resend(
            namespace="n-test",
            topic="t-test",
            subscription="s-test",
            to_topic="t2-test",
            max_scan_messages=700,
            rate_per_second=13,
            wave_size=17,
            watch_seconds=120,
            stop_on_bounce_percent=2.5,
            snapshot_path="snap-test.ndjson",
        )
    ]

    # Then
    assert events[:4] == [
        DlqResult(schema=1, sequence_number=1042, status="resent", message_id="msg-1"),
        DlqResult(
            schema=1,
            sequence_number=1043,
            status="failed",
            message_id="msg-2",
            error="the message is too large",
        ),
        DlqResult(
            schema=1,
            sequence_number=1044,
            status="sent_not_completed",
            message_id="msg-3",
            error="lock lost",
        ),
        DlqResult(
            schema=1, sequence_number=1045, status="skipped_duplicate_window", message_id="msg-4"
        ),
    ]
    assert events[4] == ResendSummary(
        schema=1,
        resent=1,
        failed=1,
        sent_not_completed=1,
        skipped=1,
        bounced=1,
        is_stopped_on_bounce=True,
        not_reached=2,
        is_scan_limit_reached=True,
        snapshot_path="snap.ndjson",
    )
    assert fake.read_arguments() == [
        "dlq", "resend", "--namespace", "n-test", "--topic", "t-test", "--subscription", "s-test",
        "--to-topic", "t2-test", "--max-scan", "700", "--rate", "13", "--wave", "17",
        "--watch", "120s", "--stop-on-bounce", "2.5", "--snapshot", "snap-test.ndjson",
        "--yes", "--output", "json",
    ]  # fmt: skip


async def test_dlq_resend_passes_a_watch_given_as_a_go_duration_on_as_it_is(
    fake_asbctl: FakeFactory,
):
    # Given
    fake = fake_asbctl()

    # When
    [event async for event in dlq_resend(namespace="n-test", queue="q-test", watch_seconds="3m")]

    # Then
    arguments = fake.read_arguments()
    assert arguments[arguments.index("--watch") + 1] == "3m"


async def test_dlq_resend_raises_after_the_summary_when_it_stopped_on_bounces(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake_asbctl(
        stdout=read_contract("dlq_resend_result.ndjson"),
        stderr=(
            '{"schema":1,"error":{"code":"stopped_on_bounce","message":'
            '"stopped before the next wave: 3 of 40 resent messages came back"}}\n'
        ),
        exit_code=3,
    )
    events = dlq_resend(namespace="n-test", queue="q-test")

    # When
    received = [await anext(events) for _ in range(5)]
    with pytest.raises(AsbctlError) as raised:
        await anext(events)

    # Then
    assert len(received) == 5
    assert (raised.value.code, raised.value.exit_code) == ("stopped_on_bounce", 3)


async def test_dlq_purge_yields_each_result_then_the_summary(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("dlq_purge_result.ndjson"))

    # When
    events = [
        event
        async for event in dlq_purge(
            namespace="n-test",
            queue="q-test",
            reason="Expired",
            max_messages=9,
            max_scan_messages=800,
        )
    ]

    # Then
    first, second, summary = events
    assert isinstance(first, DlqResult)
    assert isinstance(second, DlqResult)
    assert [first.status, second.status] == ["purged", "failed"]
    assert summary == PurgeSummary(
        schema=1,
        purged=1,
        failed=1,
        not_reached=0,
        is_scan_limit_reached=False,
        snapshot_path="snap.ndjson",
    )
    assert fake.read_arguments() == [
        "dlq", "purge", "--namespace", "n-test", "--queue", "q-test", "--reason", "Expired",
        "--max", "9", "--max-scan", "800", "--yes", "--output", "json",
    ]  # fmt: skip


async def test_dlq_resend_plan_raises_a_usage_error_for_a_subscription_without_a_target(
    fake_asbctl: FakeFactory,
):
    # Given
    fake_asbctl(
        stderr=(
            '{"schema":1,"error":{"code":"usage","message":'
            '"a subscription\'s dead-letter queue has no default target: pass --to-topic"}}\n'
        ),
        exit_code=2,
    )

    # When
    with pytest.raises(AsbctlError) as raised:
        await dlq_resend_plan(namespace="n-test", topic="t-test", subscription="s-test")

    # Then
    assert raised.value.exit_code == 2
    assert "no default target" in raised.value.message


async def test_dlq_resend_plan_forwards_every_argument(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("dlq_resend_plan.json"))

    # When
    await dlq_resend_plan(
        namespace="ns-a",
        queue="q-a",
        topic="t-a",
        subscription="s-a",
        to_queue="tq-a",
        to_topic="tt-a",
        payload_type_id="P-a",
        reason="R-a",
        max_messages=3,
        is_message_id_replaced=True,
        snapshot_path="snap-a.ndjson",
        is_dry_run=True,
    )

    # Then
    assert fake.read_arguments() == [
        "dlq", "resend", "--namespace", "ns-a", "--queue", "q-a", "--topic", "t-a",
        "--subscription", "s-a", "--to-queue", "tq-a", "--to-topic", "tt-a",
        "--payload-type-id", "P-a", "--reason", "R-a", "--max", "3", "--new-message-id",
        "--snapshot", "snap-a.ndjson", "--dry-run", "--output", "json",
    ]  # fmt: skip


async def test_dlq_purge_plan_forwards_every_argument(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("dlq_purge_plan.json"))

    # When
    await dlq_purge_plan(
        namespace="ns-b",
        queue="q-b",
        topic="t-b",
        subscription="s-b",
        payload_type_id="P-b",
        reason="R-b",
        max_messages=4,
        snapshot_path="snap-b.ndjson",
        is_dry_run=True,
    )

    # Then
    assert fake.read_arguments() == [
        "dlq", "purge", "--namespace", "ns-b", "--queue", "q-b", "--topic", "t-b",
        "--subscription", "s-b", "--payload-type-id", "P-b", "--reason", "R-b", "--max", "4",
        "--snapshot", "snap-b.ndjson", "--dry-run", "--output", "json",
    ]  # fmt: skip


async def test_dlq_resend_forwards_every_argument(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl()

    # When
    [
        event
        async for event in dlq_resend(
            namespace="ns-c",
            queue="q-c",
            topic="t-c",
            subscription="s-c",
            to_queue="tq-c",
            to_topic="tt-c",
            payload_type_id="P-c",
            reason="R-c",
            max_messages=5,
            max_scan_messages=6,
            rate_per_second=7,
            wave_size=8,
            watch_seconds=9.5,
            stop_on_bounce_percent=1.5,
            is_message_id_replaced=True,
            snapshot_path="snap-c.ndjson",
        )
    ]

    # Then
    assert fake.read_arguments() == [
        "dlq", "resend", "--namespace", "ns-c", "--queue", "q-c", "--topic", "t-c",
        "--subscription", "s-c", "--to-queue", "tq-c", "--to-topic", "tt-c",
        "--payload-type-id", "P-c", "--reason", "R-c", "--max", "5", "--max-scan", "6",
        "--rate", "7", "--wave", "8", "--watch", "9.5s", "--stop-on-bounce", "1.5",
        "--new-message-id", "--snapshot", "snap-c.ndjson", "--yes", "--output", "json",
    ]  # fmt: skip


async def test_dlq_purge_forwards_every_argument(fake_asbctl: FakeFactory):
    # Given
    fake = fake_asbctl()

    # When
    [
        event
        async for event in dlq_purge(
            namespace="ns-d",
            queue="q-d",
            topic="t-d",
            subscription="s-d",
            payload_type_id="P-d",
            reason="R-d",
            max_messages=3,
            max_scan_messages=4,
            snapshot_path="snap-d.ndjson",
        )
    ]

    # Then
    assert fake.read_arguments() == [
        "dlq", "purge", "--namespace", "ns-d", "--queue", "q-d", "--topic", "t-d",
        "--subscription", "s-d", "--payload-type-id", "P-d", "--reason", "R-d", "--max", "3",
        "--max-scan", "4", "--snapshot", "snap-d.ndjson", "--yes", "--output", "json",
    ]  # fmt: skip


async def test_dlq_helpers_add_no_flag_for_an_argument_that_was_left_out(
    fake_asbctl: FakeFactory, read_contract: Callable[[str], str]
):
    # Given
    fake = fake_asbctl(stdout=read_contract("dlq_resend_plan.json"))

    # When
    await dlq_resend_plan()
    resend_plan_arguments = fake.read_arguments()
    fake_asbctl(stdout=read_contract("dlq_purge_plan.json"))
    await dlq_purge_plan()
    purge_plan_arguments = fake.read_arguments()
    fake_asbctl()
    [event async for event in dlq_resend()]
    resend_arguments = fake.read_arguments()
    [event async for event in dlq_purge()]
    purge_arguments = fake.read_arguments()

    # Then
    assert resend_plan_arguments == ["dlq", "resend", "--output", "json"]
    assert purge_plan_arguments == ["dlq", "purge", "--output", "json"]
    assert resend_arguments == ["dlq", "resend", "--yes", "--output", "json"]
    assert purge_arguments == ["dlq", "purge", "--yes", "--output", "json"]


async def test_every_dlq_helper_applies_the_timeout(fake_asbctl: FakeFactory):
    # Given
    fake_asbctl(stdout='{"schema":1}\n', sleep_seconds=60)

    # When
    outcomes = []
    for call in (dlq_resend_plan, dlq_purge_plan):
        with pytest.raises(TimeoutError):
            await call(timeout_seconds=0.5)
        outcomes.append("timed out")
    for call in (dlq_resend, dlq_purge):
        with pytest.raises(TimeoutError):
            [event async for event in call(timeout_seconds=0.5)]
        outcomes.append("timed out")

    # Then
    assert outcomes == ["timed out"] * 4


def test_dlq_plan_from_record_reads_a_dedupe_without_a_window():
    # Given
    record = {
        "schema": 1,
        "command": "dlq resend",
        "source": {"queue": "orders"},
        "selected": 0,
        "groups": [],
        "warnings": [],
        "dry_run": True,
        "dedupe": {"enabled": False, "assumed": False, "repeated_in_selection": 0, "in_ledger": 0},
    }

    # When
    plan = dlq_plan_from_record(record)

    # Then
    assert plan.dedupe == Dedupe(
        is_enabled=False, is_assumed=False, repeated_in_selection=0, in_ledger=0, window_seconds=0
    )
    assert plan.snapshot_path is None
    assert plan.target is None


def test_dlq_plan_from_record_reads_every_group_field():
    # Given
    record = {
        "schema": 1,
        "command": "dlq purge",
        "source": {"topic": "events", "subscription": "billing"},
        "selected": 6,
        "groups": [{"payload_type_id": "Invoice", "dead_letter_reason": "Expired", "count": 6}],
        "warnings": ["careful"],
        "dry_run": False,
        "snapshot": "snap-e.ndjson",
    }

    # When
    plan = dlq_plan_from_record(record)

    # Then
    assert plan == DlqPlan(
        schema=1,
        command="dlq purge",
        source=Location(topic="events", subscription="billing"),
        selected=6,
        groups=(DlqGroup("Invoice", "Expired", 6),),
        warnings=("careful",),
        is_dry_run=False,
        snapshot_path="snap-e.ndjson",
    )


def test_dlq_result_from_record_leaves_the_optional_fields_empty():
    # Given
    record = {"schema": 1, "sequence_number": 12, "status": "purged"}

    # When
    result = dlq_result_from_record(record)

    # Then
    assert result == DlqResult(schema=1, sequence_number=12, status="purged")
