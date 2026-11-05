# ASBctl

A thin command-line tool for Azure Service Bus message work: peek, dead-letter summary, export/import and dead-letter resend. `az servicebus` covers the infrastructure side (namespaces, queues, topics, rules); this tool does not. `peek`, `export`, `send` and the dead-letter commands `dlq summary`, `dlq resend` and `dlq purge` exist. [docs/dead-letter.md](docs/dead-letter.md) has the rules for the dead-letter commands.

## Install

```
go install github.com/marcinkaczmarek10/asbctl/cmd/asbctl@latest
```

## Usage

```
asbctl works with Azure Service Bus messages.

Usage:
  asbctl <command> [flags]

Commands:
  peek     peek queue and subscription messages without removing them
  dlq      work with dead-letter queues
  export   write the messages of a queue or subscription as NDJSON records
  send     send NDJSON records to a queue or topic
  version  print the asbctl version
  help     print this help

Run "asbctl <command> -h" for the flags of a command.
```

Output is a table by default and NDJSON with `--output json`. The record shape, error object and exit codes are in [docs/output-format.md](docs/output-format.md). The rules for dead-letter commands are in [docs/dead-letter.md](docs/dead-letter.md).

## Python

`python/` holds a zero-dependency adapter that runs `asbctl --output json` and returns typed results. It needs Python 3.13 or later, and finds the binary through `ASBCTL_BIN` or `PATH`.

```python
from asbctl import AsbctlError, dlq_summary, peek

summary = dlq_summary(namespace="contoso", queue="orders")
for group in summary.groups:
    print(group.count, group.payload_type_id or "(none)", group.dead_letter_reason)
for message in peek(namespace="contoso", queue="orders", dlq=True, payload_type_id="OrderPlaced", max_messages=10):
    print(message.message_id, message.body_text)
```

The helpers are `peek`, `export`, `export_to_file`, `dlq_summary`, `send_plan`, `send`, `dlq_resend_plan`, `dlq_resend`, `dlq_purge_plan` and `dlq_purge`. `send` takes `Message` objects or dicts in the export format and yields one `SendResult` per message. Failures raise `AsbctlError` with the `code`, `message` and `exit_code` that `asbctl` reported.

## Development

`go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...` must be clean before a merge. For the Python adapter, run `uv run ruff check`, `uv run ruff format --check`, `uv run ty check` and `uv run pytest` from `python/`. The hooks in `.pre-commit-config.yaml` run the same checks through [prek](https://github.com/j178/prek). The `go` line in `go.mod` is the oldest patched toolchain, so raise it when govulncheck reports a standard library fix.

## Licence

MIT, see [LICENSE](LICENSE).
