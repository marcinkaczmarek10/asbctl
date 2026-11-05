"""Typed helpers for dlq resend and dlq purge, as described in docs/dead-letter.md."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from asbctl.core import run


@dataclass(frozen=True, slots=True)
class Location:
    """A queue, or a topic and optionally one of its subscriptions."""

    queue: str | None = None
    topic: str | None = None
    subscription: str | None = None


@dataclass(frozen=True, slots=True)
class DlqGroup:
    """Selected messages that share a payload type and a reason. An empty id means no property."""

    payload_type_id: str
    dead_letter_reason: str
    count: int


@dataclass(frozen=True, slots=True)
class Dedupe:
    """How the duplicate detection guard treats the selection.

    assumed means that the settings were unreadable.
    """

    enabled: bool
    assumed: bool
    repeated_in_selection: int
    in_ledger: int
    window_seconds: int = 0


@dataclass(frozen=True, slots=True)
class DlqPlan:
    """What a dlq command would do. target and dedupe are only set for a resend."""

    schema: int
    command: str
    source: Location
    selected: int
    groups: tuple[DlqGroup, ...]
    warnings: tuple[str, ...]
    dry_run: bool
    snapshot: str | None = None
    target: Location | None = None
    dedupe: Dedupe | None = None


@dataclass(frozen=True, slots=True)
class DlqResult:
    """The outcome for one selected message.

    status is resent, failed, sent_not_completed or skipped_duplicate_window for a resend, and
    purged or failed for a purge.
    """

    schema: int
    sequence_number: int
    status: str
    message_id: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ResendSummary:
    """The last object of a resend.

    not_reached counts selected messages that were never received.
    """

    schema: int
    resent: int
    failed: int
    sent_not_completed: int
    skipped: int
    bounced: int
    stopped_on_bounce: bool
    not_reached: int
    snapshot: str


@dataclass(frozen=True, slots=True)
class PurgeSummary:
    """The last object of a purge."""

    schema: int
    purged: int
    failed: int
    not_reached: int
    snapshot: str


def plan_from_record(record: Mapping[str, Any]) -> DlqPlan:
    """Build a Plan from the one JSON object that a dlq command prints without --yes."""
    dedupe = record.get("dedupe")
    target = record.get("target")
    return DlqPlan(
        schema=record["schema"],
        command=record["command"],
        source=Location(**record["source"]),
        selected=record["selected"],
        groups=tuple(DlqGroup(**group) for group in record["groups"]),
        warnings=tuple(record["warnings"]),
        dry_run=record["dry_run"],
        snapshot=record.get("snapshot"),
        target=None if target is None else Location(**target),
        dedupe=None if dedupe is None else Dedupe(**dedupe),
    )


def result_from_record(record: Mapping[str, Any]) -> DlqResult:
    """Build a DlqResult from one message line."""
    return DlqResult(
        schema=record["schema"],
        sequence_number=record["sequence_number"],
        status=record["status"],
        message_id=record.get("message_id"),
        error=record.get("error"),
    )


def resend_event_from_record(record: Mapping[str, Any]) -> DlqResult | ResendSummary:
    """Build a DlqResult for a message line, or the ResendSummary for the closing object."""
    if record.get("type") != "summary":
        return result_from_record(record)
    return ResendSummary(**{name: value for name, value in record.items() if name != "type"})


def purge_event_from_record(record: Mapping[str, Any]) -> DlqResult | PurgeSummary:
    """Build a DlqResult for a message line, or the PurgeSummary for the closing object."""
    if record.get("type") != "summary":
        return result_from_record(record)
    return PurgeSummary(**{name: value for name, value in record.items() if name != "type"})


def _duration_flag(value: float | str | None) -> str | None:
    """Turn seconds into the Go duration that --watch reads. A string is passed on as it is."""
    return f"{value}s" if isinstance(value, int | float) else value


def dlq_resend_plan(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    to_queue: str | None = None,
    to_topic: str | None = None,
    payload_type_id: str | None = None,
    reason: str | None = None,
    max_messages: int | None = None,
    new_message_id: bool = False,
    snapshot: str | None = None,
    dry_run: bool = False,
    timeout: float | None = None,
) -> DlqPlan:
    """Return what dlq resend would do. Nothing is changed, and no snapshot is written."""
    records = run(
        "dlq",
        "resend",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        to_queue=to_queue,
        to_topic=to_topic,
        payload_type_id=payload_type_id,
        reason=reason,
        max=max_messages,
        new_message_id=new_message_id,
        snapshot=snapshot,
        dry_run=dry_run,
    )
    return plan_from_record(next(records))


def dlq_resend(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    to_queue: str | None = None,
    to_topic: str | None = None,
    payload_type_id: str | None = None,
    reason: str | None = None,
    max_messages: int | None = None,
    rate: int | None = None,
    wave: int | None = None,
    watch: float | str | None = None,
    stop_on_bounce: float | None = None,
    new_message_id: bool = False,
    snapshot: str | None = None,
    timeout: float | None = None,
) -> Iterator[DlqResult | ResendSummary]:
    """Resend dead-lettered messages and yield the result of each, then the summary.

    A snapshot is written first. AsbctlError is raised after the summary when not every selected
    message was resent, with exit code 3 if some were and 1 if none were. Call dlq_resend_plan first
    to see the plan.
    """
    records = run(
        "dlq",
        "resend",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        to_queue=to_queue,
        to_topic=to_topic,
        payload_type_id=payload_type_id,
        reason=reason,
        max=max_messages,
        rate=rate,
        wave=wave,
        watch=_duration_flag(watch),
        stop_on_bounce=stop_on_bounce,
        new_message_id=new_message_id,
        snapshot=snapshot,
        yes=True,
    )
    return (resend_event_from_record(record) for record in records)


def dlq_purge_plan(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    payload_type_id: str | None = None,
    reason: str | None = None,
    max_messages: int | None = None,
    snapshot: str | None = None,
    dry_run: bool = False,
    timeout: float | None = None,
) -> DlqPlan:
    """Return what dlq purge would do. Nothing is changed, and no snapshot is written."""
    records = run(
        "dlq",
        "purge",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        payload_type_id=payload_type_id,
        reason=reason,
        max=max_messages,
        snapshot=snapshot,
        dry_run=dry_run,
    )
    return plan_from_record(next(records))


def dlq_purge(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    payload_type_id: str | None = None,
    reason: str | None = None,
    max_messages: int | None = None,
    snapshot: str | None = None,
    timeout: float | None = None,
) -> Iterator[DlqResult | PurgeSummary]:
    """Remove dead-lettered messages and yield the result of each, then the summary.

    A snapshot is written first. AsbctlError is raised after the summary when not every selected
    message was purged. Call dlq_purge_plan first to see the plan.
    """
    records = run(
        "dlq",
        "purge",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        payload_type_id=payload_type_id,
        reason=reason,
        max=max_messages,
        snapshot=snapshot,
        yes=True,
    )
    return (purge_event_from_record(record) for record in records)
