# ASBctl

A thin command-line tool for Azure Service Bus message work: peek, dead-letter summary, export/import and dead-letter resend. `az servicebus` covers the infrastructure side (namespaces, queues, topics, rules); this tool does not. Only `version` and `help` exist so far, the Service Bus commands are next.

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
  version  print the asbctl version
  help     print this help
```

## Licence

MIT, see [LICENSE](LICENSE).
