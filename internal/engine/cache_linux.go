//go:build linux

package engine

import (
	"os"

	"golang.org/x/sys/unix"
)

const dropWindowBytes = 64 << 20

// writeDropper asynchronously flushes and evicts sequential snapshot pages from
// the OS page cache so restoring a multi-GiB dataset does not evict hot indexes.
type writeDropper struct {
	fd      int
	pending int64 // start offset of the window currently being written back (-1 = none)
	flushed int64 // end offset already scheduled for writeback
}

func newWriteDropper(f *os.File) writeDropper {
	return writeDropper{fd: int(f.Fd()), pending: -1}
}

func (d *writeDropper) advance(written int64) {
	if written-d.flushed < dropWindowBytes {
		return
	}
	if d.pending >= 0 {
		n := d.flushed - d.pending
		_ = unix.SyncFileRange(d.fd, d.pending, n, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
		_ = unix.Fadvise(d.fd, d.pending, n, unix.FADV_DONTNEED)
	}
	n := written - d.flushed
	_ = unix.SyncFileRange(d.fd, d.flushed, n, unix.SYNC_FILE_RANGE_WRITE)
	d.pending = d.flushed
	d.flushed = written
}

func (d *writeDropper) finish(written int64) {
	if d.pending >= 0 {
		n := d.flushed - d.pending
		_ = unix.SyncFileRange(d.fd, d.pending, n, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
		_ = unix.Fadvise(d.fd, d.pending, n, unix.FADV_DONTNEED)
	}
	if written > d.flushed {
		n := written - d.flushed
		_ = unix.SyncFileRange(d.fd, d.flushed, n, unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
		_ = unix.Fadvise(d.fd, d.flushed, n, unix.FADV_DONTNEED)
	}
}
