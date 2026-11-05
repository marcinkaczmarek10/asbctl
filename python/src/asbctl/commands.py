"""Typed helpers for the asbctl commands."""

from collections.abc import Iterator

from asbctl.core import run
from asbctl.models import Message, Summary, message_from_record, summary_from_record


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
