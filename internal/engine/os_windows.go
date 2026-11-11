package engine

import (
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

// NoFollow makes an open refuse a symbolic link in the last component of the path: Windows
// opens the reparse point itself, never its target.
const NoFollow = syscall.FILE_FLAG_OPEN_REPARSE_POINT

// Lock takes an exclusive lock on f without waiting; it holds until f is closed.
func Lock(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
}
