//go:build !gardenfail

package garden

// failpoint marks a commit boundary for the process-kill harness. It is
// empty, and inlined away, unless built with -tags gardenfail.
func failpoint(string) {}
