# Dead-letter operations

A dead-letter run that resends blindly can fail without any error: the messages return to the queue they came from, and nothing says which ones or why. These rules fix that. They are the contract for the `dlq` commands, and they hold for every one of them. `dlq summary` shows what a dead-letter queue holds, `dlq resend` sends messages back, and `dlq purge` removes them. The record and output formats are in [output-format.md](output-format.md).

## Failure modes these rules prevent

- A blind bulk replay into a consumer that cannot process the messages: they return as `MaxDeliveryCountExceeded` and the run still looks successful. Rule: a canary with `--max`, then `--watch` and `--stop-on-bounce`.
- Raising `MaxDeliveryCount` when the consumer itself fails only delays the same bounce. Rule: read `dlq summary` first, which shows reason and delivery counts, fix the consumer, then resend.
- One DLQ holds several message types and only some are worth replaying. Rule: `--payload-type-id` and `--reason` filters on resend and purge.
- Duplicate detection drops a resent message that keeps its `MessageId` inside the window, and completing the DLQ copy then loses the message. Rule: the window check, and complete only after a confirmed send.
- A replay floods a consumer, or its database, that is already struggling. Rule: `--rate`.
- A purge or resend without a copy leaves no way back. Rule: a snapshot by default.

## Selection

`dlq resend` and `dlq purge` peek the dead-letter queue and pick messages with `--payload-type-id` and `--reason`, which match the `PayloadTypeId` application property and `dead_letter_reason` exactly. `--max` caps how many messages are picked, and 0, the default, means all that match. The picked messages are the selection. A message that no filter picks is never touched.

## Snapshot

Before a command changes anything it writes the selection to a snapshot file. It is on by default. The default path is `asbctl-snapshot-<entity>-<UTC timestamp>.ndjson` in the working directory, and `--snapshot <path>` chooses another. The file is NDJSON in the message record format, one record per selected message, and it is what `asbctl export` writes. `asbctl send` reads the same format, so a snapshot can be sent back.

The snapshot is written to `<path>.tmp` and renamed when the selection is complete, so a snapshot that exists is whole. An existing file is an error. If the snapshot cannot be written, the command stops before it changes any message. Its path is in the plan and in the closing summary.

## Plan and confirmation

Both commands take `--dry-run` and need `--yes`. Without `--yes` they print the plan and exit 0, and change nothing. The plan has the count per payload type and reason, the source and target, the snapshot path, and for resend the duplicate detection status and any warnings. There is no interactive prompt, so a terminal and a script follow the same rule.

`--dry-run` prints the same plan with or without `--yes`. It sends, completes and removes nothing, and writes no snapshot. Both read the dead-letter queue to build the plan, so they need to connect.

## Processing

A command receives the dead-letter queue under peek-lock and acts only on messages whose sequence number is in the selection. It never receives in delete mode: a message leaves the queue only when its own `Complete` succeeded.

A message that is not acted on stays locked until the run ends, and only then is abandoned. Abandoning it at once would put it back at the head of the queue, and the next receive would return it before any message behind it, so a selection that sits behind other messages would never be reached. The cost is that the lock expires after the lock duration, which bounds how far one run reads through messages it skips. The SDK keeps the whole received message behind a lock, body included, and offers no way to drop it, so memory grows with the number of messages held and their size. `--max-scan` caps how many messages a run holds, 50000 by default, and 0 means no cap. When a run reaches it with selected messages left, it stops, releases what it holds, and reports the rest as `not_reached` with `scan_limit_reached` set. A selected message that a run never received is counted as `not_reached` in the summary, and the exit code is then 3. Run the command again for the rest.

A message that cannot be abandoned when the run ends comes back when its lock expires. The run counts those and logs one warning with the count.

A run stops when every selected message is handled, when a receive returns nothing, when a receive returns only messages it has seen before, or when it has received as many messages as the selection's peek counted up to the last selected one. No receive asks for more than that count still allows. The Azure SDK keeps the unused part of a receive request open on the link, and the broker redelivers the messages a run abandons into that open request; the ones in flight when the run ends stay locked for the lock duration, and a run started in that window finds nothing to receive. If messages in the selected range disappear between the selection and the receive, for example because another consumer took them, part of a request can stay open and the held messages can stay locked until their lock expires. A receive batch holds at most what can be sent in half the lock duration at `--rate`, so no message loses its lock while it waits. When the lock duration cannot be read, 30 seconds is assumed.

## Resend

`dlq resend` builds a message from each received one, sends it, and completes the dead-letter copy only after the send succeeded.

The target is the queue of the dead-letter queue by default. The dead-letter queue of a subscription has no default target: pass `--to-topic` or `--to-queue`. A send to a topic reaches every matching subscription, not only the one the messages came from, and the help text and the plan say so. `--to-queue` and `--to-topic` cannot point at a subscription.

The new message keeps its `MessageId`, its other fields and every application property, except the dead-letter reason and description that Service Bus adds. A body of several AMQP data sections is refused and reported as `failed`, because only the first would survive the copy.

- A send that fails is reported as `failed`. The message is abandoned when the run ends.
- A send that succeeds and a `Complete` that fails is reported as `sent_not_completed`. The copy exists and the dead-letter message will reappear, so the status is explicit and is never swallowed. Resending it again would send a second copy.
- `--rate N` sends at most N messages per second.

### Waves and bounces

Messages go out in waves of `--wave` messages, 100 by default. After each wave the command reads the source dead-letter queue forward from where it last looked, and counts a message as a bounce when it was enqueued since the run began and its `MessageId`, or the original id in `asbctl-original-message-id`, is one that was resent. When the share of bounces among all resent messages reaches `--stop-on-bounce` percent, the command stops before the next wave and exits 3. The default is 5, and 0 turns it off. `--watch <duration>` keeps looking for bounces for that long after the last wave, every 5 seconds, and counts them in the result.

Bounce detection reads the source dead-letter queue only. When the target is another entity, a message that is dead-lettered again there is not seen, and the plan warns about it. It also assumes that a message dead-lettered again gets a higher sequence number than everything already in the queue.

## MessageId and duplicate detection

Resend keeps the `MessageId` by default. It also keeps every application property, as the safety rules in [output-format.md](output-format.md) say.

When the target has duplicate detection, a send that repeats a `MessageId` inside the detection window is accepted and then dropped. So when ids are kept, resend refuses a message and leaves its dead-letter copy in place if its `MessageId`:

- occurs twice in the selection, in which case both are refused, or
- was resent to the same namespace and target inside the window, according to the resend ledger.

A refused message is reported as `skipped_duplicate_window`. The ledger is an append-only NDJSON file at `$XDG_STATE_HOME/asbctl/resend-ledger.ndjson`, or `~/.local/state/asbctl/resend-ledger.ndjson`. Each line holds `namespace`, `target`, `message_id` and `sent_at`, and is written after each successful send. It is local to the machine, so a resend from another machine is not seen.

Reading the duplicate detection settings of the target needs Manage rights. When they cannot be read, duplicate detection is assumed on with a window of 10 minutes, and the plan says so.

`--new-message-id` assigns a new `MessageId` and stores the original in the application property `asbctl-original-message-id`. The guard then does not apply, and a consumer that deduplicates on `MessageId` will process the message twice, which the help text and the plan say.

## Purge

`dlq purge` completes each selected message under peek-lock and reports it as `purged`, or as `failed` when its `Complete` failed. It takes the same selection, snapshot, plan and confirmation as resend, and has no target, rate or waves.

## Exit codes

For both commands 0 means everything selected was handled, 3 means some of it was, and 1 means none was. For resend, 3 also means that some resent messages came back, or that the run stopped on bounces. See [output-format.md](output-format.md) for the output.
