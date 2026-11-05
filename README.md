# ASBctl

A thin command-line tool for Azure Service Bus message work: peek, dead-letter summary, export/import and dead-letter resend. `az servicebus` covers the infrastructure side (namespaces, queues, topics, rules); this tool does not. Only `peek` exists so far, the other commands are next.

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
  version  print the asbctl version
  help     print this help

Run "asbctl <command> -h" for the flags of a command.
```

Output is a table by default and NDJSON with `--output json`. The record shape, error object and exit codes are in [docs/output-format.md](docs/output-format.md). The rules for dead-letter commands are in [docs/dead-letter.md](docs/dead-letter.md).

## Licence

MIT, see [LICENSE](LICENSE).
