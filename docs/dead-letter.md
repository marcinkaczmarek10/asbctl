# Dead-letter operations

A dead-letter run that resends blindly can fail without any error: the messages return to the queue they came from, and nothing says which ones or why. These rules fix that. They are the contract for the `dlq` commands, and they hold for every one of them.

Of the `dlq` commands only `dlq summary` exists today. `dlq resend` and `dlq purge` are specified here before they are built, so that nobody builds them without the safeguards. The record and output format are in [output-format.md](output-format.md).

## Failure modes these rules prevent

- A blind bulk replay into a consumer that cannot process the messages: they return as `MaxDeliveryCountExceeded` and the run still looks successful. Rule: a canary with `--max`, then `--watch` and `--stop-on-bounce`.
- Raising `MaxDeliveryCount` when the consumer itself fails only delays the same bounce. Rule: read `dlq summary` first, which shows reason and delivery counts, fix the consumer, then resend.
- One DLQ holds several message types and only some are worth replaying. Rule: `--payload-type-id` and `--reason` filters on resend and purge.
- Duplicate detection drops a resent message that keeps its `MessageId` inside the window, and completing the DLQ copy then loses the message. Rule: the window check, and complete only after a confirmed send.
- A replay floods a consumer, or its database, that is already struggling. Rule: `--rate`.
- A purge or resend without a copy leaves no way back. Rule: a snapshot by default.

## Snapshot

`dlq resend` and `dlq purge` write a snapshot before they change anything. It is on by default, and the command prints its path. `--snapshot <path>` chooses the path. The file is NDJSON in the message record format, one record per message the command will act on.

If the snapshot cannot be written, the command stops before it changes any message.

## Plan and confirmation

Both commands take `--dry-run` and need `--yes`. Without `--yes` they print the plan and exit 0, and change nothing. The plan has the count per payload type, the target, and the snapshot path. There is no interactive prompt, so a terminal and a script follow the same rule.

`--dry-run` does the reads and prints the plan and what each message would do. It sends, completes and deletes nothing, with or without `--yes`, and writes no snapshot.

## Filters and limits

Both commands take `--payload-type-id` and `--reason`, which select messages by the `PayloadTypeId` application property and by `dead_letter_reason`. `--max` caps how many messages the command acts on. A message that no filter selects is never touched.

## Resend

`dlq resend` sends each message back to its entity and then completes the dead-letter copy.

- `--rate N` sends at most N messages per second.
- `--watch <duration>`: after the last send, the command watches the DLQ for that long for the resent `MessageId`s, and reports the ones that came back by payload type and reason.
- `--stop-on-bounce <percent>` stops the run when that share of the resent messages is back in the DLQ.

## MessageId and duplicate detection

Resend keeps the `MessageId` by default. It also keeps every application property, as the safety rules in [output-format.md](output-format.md) say.

When the entity has duplicate detection, a send that repeats a `MessageId` inside the detection window is accepted and then dropped. So resend refuses a message that it already resent inside that window, reports it as skipped, and leaves its DLQ copy in place. It completes a DLQ copy only after the send succeeded.

`--new-message-id` assigns a new `MessageId` and stores the original in the application property `asbctl-original-message-id`. A consumer that deduplicates on `MessageId` will then process the message twice, and the help text says so.
