"""Typed helpers for the asbctl commands."""

import json
import os
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from asbctl.core import run
from asbctl.models import (
    ExportResult,
    Message,
    SendPlan,
    SendResult,
    Summary,
    export_result_from_record,
    message_from_record,
    record_from_message,
    send_plan_from_record,
    send_result_from_record,
    summary_from_record,
)

type Record = Message | Mapping[str, Any]


def peek(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    dlq: bool = False,
    max_messages: int | None = None,
    from_sequence: int | None = None,
    payload_type_id: str | None = None,
    timeout: float | None = None,
) -> Iterator[Message]:
    """Peek messages from a queue or a subscription, optionally its dead-letter queue.

    Messages are never locked or removed. Leave namespace out when ASBCTL_CONNECTION_STRING is set.
    """
    records = run(
        "peek",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        dlq=dlq,
        max=max_messages,
        from_sequence=from_sequence,
        payload_type_id=payload_type_id,
    )
    return (message_from_record(record) for record in records)


def dlq_summary(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    max_messages: int | None = None,
    timeout: float | None = None,
) -> Summary:
    """Group the dead-letter queue of a queue or subscription by payload type and reason."""
    records = run(
        "dlq",
        "summary",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        max=max_messages,
    )
    return summary_from_record(next(records))


def export(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    dlq: bool = False,
    max_messages: int | None = None,
    payload_type_id: str | None = None,
    timeout: float | None = None,
) -> Iterator[Message]:
    """Export the messages of a queue or a subscription, optionally its dead-letter queue.

    It is peek with a default of 10000 messages. To write the records to a file, use export_to_file.
    """
    records = run(
        "export",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        dlq=dlq,
        max=max_messages,
        payload_type_id=payload_type_id,
    )
    return (message_from_record(record) for record in records)


def export_to_file(
    file: str | os.PathLike[str],
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    dlq: bool = False,
    max_messages: int | None = None,
    payload_type_id: str | None = None,
    force: bool = False,
    timeout: float | None = None,
) -> ExportResult:
    """Export the messages to an NDJSON file, which is written whole or not at all."""
    records = run(
        "export",
        timeout=timeout,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        dlq=dlq,
        max=max_messages,
        payload_type_id=payload_type_id,
        file=os.fspath(file),
        force=force,
    )
    return export_result_from_record(next(records))


def send_plan(
    records: Iterable[Record],
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    new_message_id: bool = False,
    timeout: float | None = None,
) -> SendPlan:
    """Check the records and return what send would do. Nothing is sent."""
    plans = run(
        "send",
        timeout=timeout,
        stdin_lines=_lines_from(records),
        namespace=namespace,
        queue=queue,
        topic=topic,
        new_message_id=new_message_id,
    )
    return send_plan_from_record(next(plans))


def send(
    records: Iterable[Record],
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    rate: int | None = None,
    new_message_id: bool = False,
    timeout: float | None = None,
) -> Iterator[SendResult]:
    """Send the records to a queue or a topic and yield the result of each message.

    The records are Message objects or dicts in the export format. Every record is checked before
    anything is sent. AsbctlError is raised after the last result when some messages were not sent,
    with exit code 3 if some were sent and 1 if none were. Call send_plan first to see the plan.
    """
    results = run(
        "send",
        timeout=timeout,
        stdin_lines=_lines_from(records),
        namespace=namespace,
        queue=queue,
        topic=topic,
        rate=rate,
        new_message_id=new_message_id,
        yes=True,
    )
    return (send_result_from_record(result) for result in results)


def _lines_from(records: Iterable[Record]) -> Iterator[str]:
    for record in records:
        content = (
            record_from_message(record) if isinstance(record, Message) else {"schema": 1, **record}
        )
        yield json.dumps(content, separators=(",", ":"))
