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
    dlq_purge,
    dlq_purge_plan,
    dlq_resend,
    dlq_resend_plan,
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
        snapshot="asbctl-snapshot-orders-20261105T143005Z.ndjson",
        dedupe=Dedupe(
            enabled=True,
            window_seconds=600,
            assumed=False,
            repeated_in_selection=2,
            in_ledger=1,
        ),
        warnings=(),
        dry_run=False,
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
    assert plan.dry_run is True
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
        stopped_on_bounce=True,
        not_reached=2,
        scan_limit_reached=True,
        snapshot="snap.ndjson",
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
        scan_limit_reached=False,
        snapshot="snap.ndjson",
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
