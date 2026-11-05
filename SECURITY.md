# Security policy

## Reporting a vulnerability

Report it through GitHub private vulnerability reporting: the Security tab of this repository, then "Report a vulnerability". Do not open a public issue. Include the `asbctl version` output, the command that triggers it and what you expected to happen.

## Supported versions

Only the latest release gets fixes. There are no backports.

## Security notes

- Export, snapshot and ledger files are created with mode `0600`, and the ledger directory with `0700`. Export and snapshot files hold message bodies. The ledger holds the namespace, target, message id and send time of each resent message.
- A connection string is read only from `ASBCTL_CONNECTION_STRING`. It is never a flag, and `asbctl` never writes it anywhere.
- File paths come from the user's own flags, and `asbctl` runs with the user's own permissions. That is why gosec reports G304 and G703 on `cmd/asbctl/send.go:147,163`, `internal/export/file.go:26` and `internal/dlq/ledger.go:45,78`. They are accepted, not suppressed in code.
