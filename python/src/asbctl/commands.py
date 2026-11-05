"""Typed helpers for the asbctl commands."""

import contextlib
import json
import os
from collections.abc import AsyncGenerator, AsyncIterable, Iterable, Mapping
from typing import Any

from asbctl.core import run, run_once
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


async def peek(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    is_dead_letter: bool = False,
    max_messages: int | None = None,
    from_sequence: int | None = None,
    payload_type_id: str | None = None,
    timeout_seconds: float | None = None,
) -> AsyncGenerator[Message]:
    """Peek messages from a queue or a subscription, optionally its dead-letter queue.

    Messages are never locked or removed. Leave namespace out when ASBCTL_CONNECTION_STRING is set.
    """
    async with contextlib.aclosing(
        run(
            "peek",
            timeout_seconds=timeout_seconds,
            namespace=namespace,
            queue=queue,
            topic=topic,
            subscription=subscription,
            dlq=is_dead_letter,
            max=max_messages,
            from_sequence=from_sequence,
            payload_type_id=payload_type_id,
        )
    ) as records:
        async for record in records:
            yield message_from_record(record)


async def dlq_summary(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    max_messages: int | None = None,
    timeout_seconds: float | None = None,
) -> Summary:
    """Group the dead-letter queue of a queue or subscription by payload type and reason."""
    record = await run_once(
        "dlq",
        "summary",
        timeout_seconds=timeout_seconds,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        max=max_messages,
    )
    return summary_from_record(record)


async def export(
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    is_dead_letter: bool = False,
    max_messages: int | None = None,
    payload_type_id: str | None = None,
    timeout_seconds: float | None = None,
) -> AsyncGenerator[Message]:
    """Export the messages of a queue or a subscription, optionally its dead-letter queue.

    It is peek with a default of 10000 messages. To write the records to a file, use export_to_file.
    """
    async with contextlib.aclosing(
        run(
            "export",
            timeout_seconds=timeout_seconds,
            namespace=namespace,
            queue=queue,
            topic=topic,
            subscription=subscription,
            dlq=is_dead_letter,
            max=max_messages,
            payload_type_id=payload_type_id,
        )
    ) as records:
        async for record in records:
            yield message_from_record(record)


async def export_to_file(
    file_path: str | os.PathLike[str],
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    subscription: str | None = None,
    is_dead_letter: bool = False,
    max_messages: int | None = None,
    payload_type_id: str | None = None,
    can_overwrite: bool = False,
    timeout_seconds: float | None = None,
) -> ExportResult:
    """Export the messages to an NDJSON file, which is written whole or not at all."""
    record = await run_once(
        "export",
        timeout_seconds=timeout_seconds,
        namespace=namespace,
        queue=queue,
        topic=topic,
        subscription=subscription,
        dlq=is_dead_letter,
        max=max_messages,
        payload_type_id=payload_type_id,
        file=os.fspath(file_path),
        force=can_overwrite,
    )
    return export_result_from_record(record)


async def send_plan(
    records: AsyncIterable[Record] | Iterable[Record],
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    is_message_id_replaced: bool = False,
    timeout_seconds: float | None = None,
) -> SendPlan:
    """Check the records and return what send would do. Nothing is sent."""
    plan_record = await run_once(
        "send",
        timeout_seconds=timeout_seconds,
        stdin_lines=_lines_from(records),
        namespace=namespace,
        queue=queue,
        topic=topic,
        new_message_id=is_message_id_replaced,
    )
    return send_plan_from_record(plan_record)


async def send(
    records: AsyncIterable[Record] | Iterable[Record],
    *,
    namespace: str | None = None,
    queue: str | None = None,
    topic: str | None = None,
    rate_per_second: int | None = None,
    is_message_id_replaced: bool = False,
    timeout_seconds: float | None = None,
) -> AsyncGenerator[SendResult]:
    """Send the records to a queue or a topic and yield the result of each message.

    The records are Message objects or dicts in the export format. Every record is checked before
    anything is sent. Call send_plan first to see the plan.

    Raises:
        AsbctlError: Not every message was sent. It is raised after the last result, with exit
            code 3 if some messages were sent and 1 if none were.
    """
    async with contextlib.aclosing(
        run(
            "send",
            timeout_seconds=timeout_seconds,
            stdin_lines=_lines_from(records),
            namespace=namespace,
            queue=queue,
            topic=topic,
            rate=rate_per_second,
            new_message_id=is_message_id_replaced,
            yes=True,
        )
    ) as results:
        async for result in results:
            yield send_result_from_record(result)


async def _lines_from(records: AsyncIterable[Record] | Iterable[Record]) -> AsyncGenerator[str]:
    if isinstance(records, AsyncIterable):
        async for record in records:
            yield _line_from(record)
    else:
        for record in records:
            yield _line_from(record)


def _line_from(record: Record) -> str:
    content = (
        record_from_message(record) if isinstance(record, Message) else {"schema": 1, **record}
    )
    return json.dumps(content, separators=(",", ":"))
