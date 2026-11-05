from collections.abc import Callable

import pytest

from asbctl import (
    AsbctlError,
    DlqPlan,
    DlqResult,
    PurgeSummary,
    ResendSummary,
    dlq_purge,
    dlq_purge_plan,
    dlq_resend,
    dlq_resend_plan,
)
from asbctl.dlq import Dedupe, DlqGroup, Location

from .conftest import FakeFactory


def test_dlq_resend_plan_parses_the_contract_plan_and_passes_no_yes(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("dlq_resend_plan.json"))

    plan = dlq_resend_plan(
        namespace="n",
        queue="orders",
        payload_type_id="OrderPlaced",
        reason="Poison",
        max_messages=10,
        new_message_id=True,
        dry_run=True,
    )

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
    assert fake.arguments() == [
        "dlq", "resend", "--namespace", "n", "--queue", "orders",
        "--payload-type-id", "OrderPlaced", "--reason", "Poison", "--max", "10",
        "--new-message-id", "--dry-run", "--output", "json",
    ]  # fmt: skip
    assert "--yes" not in fake.arguments()


def test_dlq_purge_plan_parses_a_plan_without_target_or_dedupe(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(stdout=contract("dlq_purge_plan.json"))

    plan = dlq_purge_plan(namespace="n", topic="events", subscription="billing", dry_run=True)

    assert plan.command == "dlq purge"
    assert plan.source == Location(topic="events", subscription="billing")
    assert plan.target is None
    assert plan.dedupe is None
    assert plan.dry_run is True
    assert plan.selected == 4


def test_dlq_resend_yields_each_result_then_the_summary(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("dlq_resend_result.ndjson"))

    events = list(
        dlq_resend(
            namespace="n",
            topic="t",
            subscription="s",
            to_topic="t2",
            rate=20,
            wave=10,
            watch=120,
            stop_on_bounce=2.5,
            snapshot="snap.ndjson",
        )
    )

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
    assert fake.arguments() == [
        "dlq", "resend", "--namespace", "n", "--topic", "t", "--subscription", "s",
        "--to-topic", "t2", "--rate", "20", "--wave", "10", "--watch", "120s",
        "--stop-on-bounce", "2.5", "--snapshot", "snap.ndjson", "--yes", "--output", "json",
    ]  # fmt: skip


def test_a_watch_given_as_a_go_duration_is_passed_on_as_it_is(fake_asbctl: FakeFactory):
    fake = fake_asbctl()

    list(dlq_resend(namespace="n", queue="q", watch="2m"))

    assert fake.arguments()[fake.arguments().index("--watch") + 1] == "2m"


def test_dlq_purge_yields_each_result_then_the_summary(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake = fake_asbctl(stdout=contract("dlq_purge_result.ndjson"))

    events = list(dlq_purge(namespace="n", queue="q", reason="Poison", max_messages=5))

    first, second = events[:2]
    assert isinstance(first, DlqResult)
    assert isinstance(second, DlqResult)
    assert [first.status, second.status] == ["purged", "failed"]
    assert events[2] == PurgeSummary(
        schema=1,
        purged=1,
        failed=1,
        not_reached=0,
        scan_limit_reached=False,
        snapshot="snap.ndjson",
    )
    assert fake.arguments() == [
        "dlq", "purge", "--namespace", "n", "--queue", "q", "--reason", "Poison",
        "--max", "5", "--yes", "--output", "json",
    ]  # fmt: skip


def test_dlq_resend_raises_after_the_summary_when_it_stopped_on_bounces(
    fake_asbctl: FakeFactory, contract: Callable[[str], str]
):
    fake_asbctl(
        stdout=contract("dlq_resend_result.ndjson"),
        stderr=(
            '{"schema":1,"error":{"code":"stopped_on_bounce","message":'
            '"stopped before the next wave: 3 of 40 resent messages came back"}}\n'
        ),
        exit_code=3,
    )
    events = dlq_resend(namespace="n", queue="q")

    assert len([next(events) for _ in range(5)]) == 5
    with pytest.raises(AsbctlError) as raised:
        next(events)
    assert (raised.value.code, raised.value.exit_code) == ("stopped_on_bounce", 3)


def test_a_subscription_without_a_target_is_a_usage_error(fake_asbctl: FakeFactory):
    fake_asbctl(
        stderr=(
            '{"schema":1,"error":{"code":"usage","message":'
            '"a subscription\'s dead-letter queue has no default target: pass --to-topic"}}\n'
        ),
        exit_code=2,
    )

    with pytest.raises(AsbctlError) as raised:
        dlq_resend_plan(namespace="n", topic="t", subscription="s")

    assert raised.value.exit_code == 2
    assert "no default target" in raised.value.message
