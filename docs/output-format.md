# Output format

`asbctl` is called from scripts as often as from a terminal. This page is the contract for what it writes, how it fails and what it will never do to a message. Table output is for humans and can change. `--output json` is the stable one.

## Streams and modes

Data goes to stdout, logs and progress go to stderr, always. Every command takes `--output table|json`, default `table`.

In `json` mode a list streams as NDJSON: one object per line, written as it is produced, never buffered whole. A command that returns one thing writes one JSON object. A script can start reading before the command finishes.

## Message record

Keys are snake_case and empty values are omitted, except `body`, which is always present.

| Key | Type | Notes |
| --- | --- | --- |
| `schema` | int | `1` |
| `sequence_number` | int | |
| `message_id` | string | |
| `enqueued_at` | string | RFC 3339, UTC |
| `content_type` | string | |
| `correlation_id` | string | |
| `subject` | string | |
| `session_id` | string | |
| `partition_key` | string | |
| `to` | string | |
| `reply_to` | string | |
| `time_to_live_seconds` | int | |
| `delivery_count` | int | |
| `dead_letter_reason` | string | |
| `dead_letter_error_description` | string | |
| `dead_letter_source` | string | |
| `application_properties` | object | see below |
| `body_type` | string | `value` or `sequence`; absent means a data body |
| `body` | string | base64 of the raw bytes, empty for a `value` or `sequence` body |
| `body_text` | string | the body as UTF-8 text, only when it is valid UTF-8; for a `value` or `sequence` body, the JSON encoding of the section |

## Application properties

A JSON string, number or bool is written as is. Any other AMQP type is written as `{"type": "<timestamp|binary|uuid|...>", "value": "<string>"}`, so that import can restore it losslessly. Integers and finite floats are written as plain JSON numbers, so their exact AMQP width (int32 or int64, float or double) is not kept. Import writes integers as int64 and floats as double.

`PayloadTypeId` is the application property that Ev.ServiceBus, a public MIT library, uses to route a message to its handler. `asbctl` shows it, filters on it with `--payload-type-id`, and never drops it on resend.

## Dead-letter summary

`asbctl dlq summary` writes one JSON object, not a stream:

```json
{"schema":1,"entity":{"queue":"orders","available":true,"active_messages":12,"dead_letter_messages":850,"scheduled_messages":0,"transfer_dead_letter_messages":0,"max_delivery_count":10,"lock_duration":"PT1M","requires_duplicate_detection":true,"duplicate_detection_window":"PT10M"},"peeked":850,"partial":false,"groups":[{"payload_type_id":"OrderPlaced","dead_letter_reason":"MaxDeliveryCountExceeded","count":850,"first_enqueued_at":"2026-11-05T10:00:00Z","last_enqueued_at":"2026-11-05T11:00:00Z","max_delivery_count":10,"top_dead_letter_error_description":"...","sample_message_id":"..."}]}
```

`entity` has `queue`, or `topic` and `subscription`. Reading counts and settings needs Manage rights. Without them `available` is `false`, the counts and settings are omitted, and the grouping still runs. For a subscription, `requires_duplicate_detection` and `duplicate_detection_window` come from the topic, and `scheduled_messages` is omitted. `lock_duration` and `duplicate_detection_window` are ISO 8601 durations as the service returns them.

`groups` is sorted by `count` descending, then `payload_type_id`, then `dead_letter_reason`. A message with no `PayloadTypeId` property has `payload_type_id` set to the empty string, and both keys are always present. `top_dead_letter_error_description` is the most frequent one in the group, untruncated. `sample_message_id` is the first message seen in the group.

`partial` is `true` when `dead_letter_messages` is known and larger than `peeked`. When it is unknown, `partial` is `true` if `peeked` reached `--max`, because the queue may hold more. The table prints a line saying the summary covers N of M messages.

## Errors

In `json` mode a failure is one object on stderr:

```json
{"schema":1,"error":{"code":"peek_failed","message":"..."}}
```

`code` is snake_case and stable, `message` is for humans. A flag that does not parse fails before `--output` is known, so that error is plain text on stderr in either mode.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | ok |
| 1 | failed |
| 2 | usage |
| 3 | partial: a batch where some items failed |

## Authentication

`asbctl` uses `azidentity.DefaultAzureCredential`. Set `AZURE_TOKEN_CREDENTIALS=dev` to narrow it to `az` and `azd`. A connection string is read only from the environment variable `ASBCTL_CONNECTION_STRING`. It is never read from a flag and never written anywhere.

## Safety rules

These hold for every command, including the ones that do not exist yet.

- No `ReceiveAndDelete`. Every receive is peek-lock.
- Resend keeps the `message_id` and every application property.
- Resend completes the dead-letter message only after the send succeeded, and reports every complete that failed.
- Every state-changing command has `--dry-run`.
