package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/marcinkaczmarek10/asbctl/internal/bus"
	"github.com/marcinkaczmarek10/asbctl/internal/export"
)

// bindEntityFlags registers the flags that name a namespace and an entity.
func bindEntityFlags(fs *flag.FlagSet, namespace *string, target *bus.Target) {
	fs.StringVar(namespace, "namespace", "", "Service Bus namespace: a short name or a fully qualified domain name")
	fs.StringVar(&target.Queue, "queue", "", "queue to read")
	fs.StringVar(&target.Topic, "topic", "", "topic to read, with --subscription")
	fs.StringVar(&target.Subscription, "subscription", "", "subscription of --topic")
}

func validateEntity(namespace string, target bus.Target, hasConnectionString bool) error {
	switch {
	case namespace == "" && !hasConnectionString:
		return errors.New("--namespace is required")
	case target.Queue == "" && target.Topic == "":
		return errors.New("one of --queue or --topic with --subscription is required")
	case target.Queue != "" && (target.Topic != "" || target.Subscription != ""):
		return errors.New("--queue cannot be combined with --topic or --subscription")
	case target.Queue == "" && target.Subscription == "":
		return errors.New("--topic needs --subscription")
	}
	return nil
}

// parseFlagSet parses args quietly. On -h it writes usage and the flag defaults to helpOut and returns flag.ErrHelp.
func parseFlagSet(fs *flag.FlagSet, args []string, usage string, helpOut io.Writer) error {
	fs.SetOutput(io.Discard)
	err := fs.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(helpOut, usage)
		fs.SetOutput(helpOut)
		fs.PrintDefaults()
	}
	return err
}

// reportUsageError writes a usage error in the requested format and returns the usage exit code.
func reportUsageError(stderr io.Writer, format export.Format, err error, helpHint string) int {
	export.WriteError(stderr, format, "usage", err)
	if format != export.FormatJSON {
		fmt.Fprintln(stderr, helpHint)
	}
	return exitUsage
}
