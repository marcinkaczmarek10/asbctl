package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

const usageText = `asbctl works with Azure Service Bus messages.

Usage:
  asbctl <command> [flags]

Commands:
  peek     peek queue and subscription messages without removing them
  version  print the asbctl version
  help     print this help

Run "asbctl <command> -h" for the flags of a command.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, logger)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, logger *slog.Logger) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "asbctl: no command given")
		fmt.Fprint(stderr, usageText)
		return exitUsage
	}

	name, rest := args[0], args[1:]
	logger.DebugContext(ctx, "dispatching command", "command", name)

	switch name {
	case "version":
		return runVersion(rest, stdout, stderr)
	case "peek":
		return runPeek(ctx, rest, stdout, stderr, logger)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usageText)
		return exitOK
	default:
		fmt.Fprintf(stderr, "asbctl: unknown command %q\n", name)
		fmt.Fprint(stderr, usageText)
		return exitUsage
	}
}

func runVersion(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "asbctl version: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}

	fmt.Fprintln(stdout, moduleVersion())
	return exitOK
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return "(devel)"
	}
	return info.Main.Version
}
