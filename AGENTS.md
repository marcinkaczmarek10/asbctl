# AGENTS.md

Instructions for AI coding agents working in this repo.

## What this is

ASBctl is a Go CLI for Azure Service Bus message work (peek, dead-letter summary, export, send,
DLQ resend and purge), with an async Python adapter in `python/`. It is a personal open-source
project under MIT licence.

## Layout

```
cmd/asbctl/         subcommand entry points
internal/bus/       Service Bus client, receiver, sender and admin wrappers; consumer-side interfaces
internal/export/    message record, NDJSON/table writers, outgoing builder, file writer, summary aggregation
internal/dlq/       drain loop, resend, purge, bounce detection, selection, ledger, result output
python/src/asbctl/  async adapter: core.py (run), models.py, commands.py, dlq.py, bundled.py, cli.py
python/tests/       pytest suite, fake asbctl binary
python/scripts/     wheel builder
testdata/contract/  shared fixtures: Go tests write them, Python tests parse them
docs/               output-format.md (record and error schema), dead-letter.md (safety rules)
scripts/            third-party licence collector
.github/workflows/  ci.yml (push/PR), release.yml (tag v*)
```

## Rules

- **Go:** follow the go-standards skill when available. stdlib flag for CLI, stdlib testing
  with table-driven tests and hand-written fakes, no testify. Consumer-side interfaces in the
  package that uses them, with a compile-time check (var _ Iface = (*Real)(nil)).
- **Python:** async everywhere (asyncio.create_subprocess_exec, no threads). Small immutable
  values are NamedTuple. Converters named result_from_source. Zero runtime dependencies.
  Tooling: uv, ruff check + format (rules include S and I), ty, pytest + pytest-asyncio.
- **Identity:** commits authored as marcin.kacz10@gmail.com (repo-local git config). Do not
  change git config or credentials. Do not push.
- **Commits:** type(scope): subject, imperative, lower case, no trailing period. Body explains
  the problem and the reasoning. No work-item ids.
- **Mutation testing before adding tests:** Gremlins for Go (--diff origin/master), mutmut 3.8.1
  for Python (run in a copy under /tmp; leave no config or mutants/ behind). Add a test only
  to kill a surviving mutant or cover new behaviour.
- **uv commands:** run with UV_EXTRA_INDEX_URL and UV_NATIVE_TLS unset. The shell may point
  these at a private feed that must never reach uv.lock.
- **No ReceiveAndDelete anywhere.** Resend keeps the MessageId, completes only after a confirmed
  send, and reports every failed complete. These rules are in docs/dead-letter.md.
- **Checks before finishing:** gofmt -l ., go vet ./..., go test ./...,
  govulncheck@v1.9.0 ./... (must be clean), and from python/: uv run ruff check,
  uv run ruff format --check, uv run ty check, uv run pytest -W error.

## Testing

Unit tests use fakes. No live Azure in the test suite. The Service Bus emulator is not wired yet.
Contract tests in testdata/contract/ are shared between Go and Python: a Go test writes the
fixture files, and pytest parses them. If the Go output format changes, the Python tests fail.

## Security

SECURITY.md covers reporting, supported versions and the accepted gosec findings. Export,
snapshot and ledger files are written 0600 because they may hold message bodies. Connection
strings come only from ASBCTL_CONNECTION_STRING, never from flags or disk.
