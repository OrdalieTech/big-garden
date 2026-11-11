//go:build unix

package engine

import (
	"os"
	"syscall"
)

// NoFollow makes an open refuse a symbolic link in the last component of the path.
const NoFollow = syscall.O_NOFOLLOW

// Lock takes an exclusive lock on f without waiting; it holds until f is closed.
func Lock(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
