//go:build !linux

package engine

import "os"

const dropWindowBytes = 64 << 20

type writeDropper struct{}

func newWriteDropper(*os.File) writeDropper { return writeDropper{} }
func (*writeDropper) advance(int64)         {}
func (*writeDropper) finish(int64)          {}
