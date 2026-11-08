// Command qual is the qualification harness (PLAN §20). It is never
// installed with the library. Each subcommand lives in its own file and exits
// nonzero on failure, on an inconclusive result and when not implemented.
package main

import (
	"errors"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: qual model|doctor|stall|faults|stress|soak|check|cost|footprint|reproducible|report [flags]")
		os.Exit(2)
	}
	var run func([]string) error
	switch os.Args[1] {
	case "model":
		run = model
	case "doctor":
		run = doctor
	case "faults":
		run = faults
	case "stall":
		run = stall
	case "stress":
		run = stress
	case "soak":
		run = soak
	case "soak-node":
		run = soakNode
	case "check":
		run = check
	case "cost":
		run = cost
	case "footprint":
		run = footprint
	case "reproducible":
		run = reproducible
	case "report":
		run = report
	default:
		fmt.Fprintf(os.Stderr, "qual: unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "qual %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

var errNotImplemented = errors.New("not implemented")
