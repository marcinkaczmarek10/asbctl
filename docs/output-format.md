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

`groups` is sorted by `count` descending, then `payload_type_id`, then `dead_letter_reason`. A message with no `PayloadTypeId` property has `payload_type_id` set to the empty string, and both keys are always present. `top_dead_letter_error_description` is the most frequent one in the group, untruncated. With up to 100 distinct descriptions in a group this is exact. Beyond that it is approximate, but any description that makes up more than 1% of the group is always among the candidates. Past 100 distinct descriptions, ties are not broken by text: the over-count decides. `sample_message_id` is the first message seen in the group.

`partial` is `true` when `dead_letter_messages` is known and larger than `peeked`. When it is unknown, `partial` is `true` if `peeked` reached `--max`, because the queue may hold more. The table prints a line saying the summary covers N of M messages.

## Export

`asbctl export` writes the messages of an entity as message records, one NDJSON line each, in the format above. It peeks, so it never locks or removes a message, and it takes the same `--dlq`, `--max` (default 10000) and `--payload-type-id` flags as `peek`. The filter runs after peeking, so skipped messages still count toward `--max`.

The records go to `--file`, or to stdout when `--file` is absent or `-`. Records are always JSON, so `--output table` is a usage error when they go to stdout. With `--file`, stdout carries the result instead: a table-mode sentence (the default), or with `--output json` one object:

```json
{"schema":1,"exported":4,"file":"orders.ndjson","partial":true}
```

`partial` is `true` when the peek reached `--max`, because the entity may hold more. The file is written to `<file>.tmp` and renamed when the export succeeds, so a failed or interrupted export leaves nothing under the final name. An existing file is an error (`file_exists`) unless `--force` is given. Progress is logged on stderr every 1000 messages peeked.

## Send

`asbctl send` reads message records, the format that `export` writes, from `--file` or from stdin, and sends each to a queue or a topic (`--queue` or `--topic`, no subscription). The whole input is checked before anything is sent. A record that does not parse is a usage error that names its line, for example `line 3: schema is 9, want 1`.

A message keeps its `message_id`, `content_type`, `correlation_id`, `subject`, `session_id`, `partition_key`, `to`, `reply_to` and `time_to_live_seconds`, and all its application properties. The broker-set fields `sequence_number`, `enqueued_at`, `delivery_count` and `dead_letter_*` are ignored. The body is the decoded `body`. For `body_type` `value` or `sequence` it is the AMQP value or sequence that `body_text` encodes.

Typed application properties are restored to their AMQP types: `timestamp`, `binary`, `uuid`, `symbol`, `float` and `double`, including `NaN` and infinity. A JSON integer becomes an int64 and any other number a float64. A float with no fractional part, such as `3.0`, is written as `3` and so comes back as an integer. A typed object inside a `value` body is restored the same way. A property of any other type is a usage error.

`--new-message-id` gives each message a new random id and keeps the original in the application property `asbctl-original-message-id`. A consumer that deduplicates on `MessageId` will then process the message twice.

Without `--yes` the command prints the plan and sends nothing. `--dry-run` prints the same plan even with `--yes`. In `json` mode the plan is one object:

```json
{"schema":1,"target":{"queue":"orders"},"total":3,"payload_types":[{"payload_type_id":"OrderPlaced","count":2},{"payload_type_id":"","count":1}],"dry_run":false}
```

With `--yes` each message is reported as it is sent, one line in `json` mode:

```json
{"schema":1,"line":2,"message_id":"msg-2","status":"failed","error":"the message is too large"}
```

`line` is the input line, `status` is `sent` or `failed`, and `error` is set only when it failed. In table mode only failures are printed, followed by one summary line. The exit code is 0 when every message was sent, 3 when some were, and 1 when none were. In `json` mode a non-zero exit also writes the error object on stderr, with code `partial`, `send_failed` or `canceled`.

Messages go out in batches of at most 100. A batch that fails marks every message in it as failed, and a message too large for any batch fails alone. `--rate N` spaces the batches so that they average N messages per second, and then a batch is at most N messages. If the run is cancelled while a batch is in flight, that batch is reported as failed although the broker may have accepted it, so a rerun can send it twice.

## Dead-letter resend and purge

`asbctl dlq resend` and `asbctl dlq purge` follow the rules in [dead-letter.md](dead-letter.md). Without `--yes`, or with `--dry-run`, each prints the plan as one JSON object and changes nothing:

```json
{"schema":1,"type":"plan","command":"dlq resend","source":{"queue":"orders"},"target":{"queue":"orders"},"selected":4,"groups":[{"payload_type_id":"OrderPlaced","dead_letter_reason":"Poison","count":3}],"snapshot":"asbctl-snapshot-orders-20261105T143005Z.ndjson","dedupe":{"enabled":true,"window_seconds":600,"assumed":false,"repeated_in_selection":2,"in_ledger":1},"warnings":[],"dry_run":false}
```

`target` and `dedupe` are only in a resend plan. `dedupe` counts the selected messages that the guard would skip: `repeated_in_selection` for those whose `MessageId` occurs twice, and `in_ledger` for those resent inside the window. `assumed` is `true` when the settings could not be read. `warnings` is a list of sentences.

When the command acts, each selected message is reported as it is handled, one line in `json` mode:

```json
{"schema":1,"sequence_number":1042,"message_id":"msg-1","status":"resent"}
```

For resend `status` is `resent`, `failed`, `sent_not_completed` or `skipped_duplicate_window`, and for purge it is `purged` or `failed`. `error` is set when a message failed or was not completed. A message that the filters did not select is not reported. In table mode only the messages that did not go well are printed.

The run ends with one summary object, which has `"type":"summary"` so that it can be told from the result lines:

```json
{"schema":1,"type":"summary","resent":3,"failed":0,"sent_not_completed":0,"skipped":1,"bounced":0,"stopped_on_bounce":false,"not_reached":0,"scan_limit_reached":false,"snapshot":"asbctl-snapshot-orders-20261105T143005Z.ndjson"}
```

The purge summary has `purged`, `failed`, `not_reached`, `scan_limit_reached` and `snapshot`. `not_reached` counts selected messages that the run never received, and `scan_limit_reached` is `true` when the run stopped at `--max-scan` with selected messages left. A non-zero exit also writes the error object on stderr, with code `partial`, `stopped_on_bounce`, `resend_failed` or `purge_failed`. A snapshot path that exists is the code `file_exists`.

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
