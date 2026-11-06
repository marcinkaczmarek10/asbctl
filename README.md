# ASBctl

A thin command-line tool for Azure Service Bus message work: peek, dead-letter summary, export/import and dead-letter resend. `az servicebus` covers the infrastructure side (namespaces, queues, topics, rules); this tool does not. `peek`, `export`, `send` and the dead-letter commands `dlq summary`, `dlq resend` and `dlq purge` exist. [docs/dead-letter.md](docs/dead-letter.md) has the rules for the dead-letter commands.

## Install

```
go install github.com/marcinkaczmarek10/asbctl/cmd/asbctl@latest
```

A tagged release also attaches archives, `checksums.txt` and an SBOM for darwin, linux and windows on amd64 and arm64.

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

`python/` holds a zero-dependency async adapter that runs `asbctl --output json` and returns typed results. It needs Python 3.13 or later. It finds the binary through `ASBCTL_BIN`, then the binary bundled in the package, then `PATH`. A release builds one wheel for each platform that carries the binary and installs an `asbctl` command that runs it. The wheel tagged `any` and the sdist carry no binary and need `ASBCTL_BIN` or `PATH`. Windows on arm64 has no wheel.

```python
import asyncio

from asbctl import dlq_summary, peek


async def main() -> None:
    summary = await dlq_summary(namespace="contoso", queue="orders")
    for group in summary.groups:
        print(group.count, group.payload_type_id or "(none)", group.dead_letter_reason)
    async for message in peek(namespace="contoso", queue="orders", is_dead_letter=True, max_messages=10):
        print(message.message_id, message.body_text)


asyncio.run(main())
```

The helpers are `peek`, `export`, `export_to_file`, `dlq_summary`, `send_plan`, `send`, `dlq_resend_plan`, `dlq_resend`, `dlq_purge_plan` and `dlq_purge`. Helpers that return many results are async iterators, and the others are awaited. `send` takes `Message` objects or dicts in the export format and yields one `SendResult` per message. Failures raise `AsbctlError` with the `code`, `message` and `exit_code` that `asbctl` reported. A missing binary raises `BinaryNotFoundError`, a subclass of `AsbctlError`. The adapter sets its code to `binary_not_found` and its exit code to 127, because no `asbctl` process ran to report them.

## Development

`go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...` must be clean before a merge. For the Python adapter, run `uv run ruff check`, `uv run ruff format --check`, `uv run ty check` and `uv run pytest` from `python/`. The hooks in `.pre-commit-config.yaml` run the same checks through [prek](https://github.com/j178/prek). `scripts/third-party-licenses.sh` regenerates `THIRD_PARTY_LICENSES`, the licence and notice text of the linked modules. Run it after a change to `go.mod`. CI fails when the file is stale. Release archives and every wheel carry it.

The `go` line in `go.mod` is the oldest patched toolchain, so raise it when govulncheck reports a standard library fix.

## Licence

MIT, see [LICENSE](LICENSE).
