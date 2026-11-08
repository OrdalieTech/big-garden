//go:build gardenfail

package garden

import (
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// GARDEN_FAILPOINT=name:n SIGKILLs the process at the nth pass through
// failpoint(name); a name ending in ".mid" kills 0-3 ms later instead, while
// the request that follows is in flight. name:n:stop sends SIGSTOP instead,
// and the process carries on once continued.
var failName, failHits, failSig = func() (string, int64, syscall.Signal) {
	f := strings.Split(os.Getenv("GARDEN_FAILPOINT")+"::", ":")
	hits, _ := strconv.ParseInt(f[1], 10, 64)
	if f[2] == "stop" {
		return f[0], max(hits, 1), syscall.SIGSTOP
	}
	return f[0], max(hits, 1), syscall.SIGKILL
}()

var failCount atomic.Int64

func failpoint(name string) {
	if name != failName || failCount.Add(1) != failHits {
		return
	}
	if strings.HasSuffix(name, ".mid") {
		go func() {
			time.Sleep(rand.N(3 * time.Millisecond))
			syscall.Kill(os.Getpid(), failSig)
		}()
		return
	}
	syscall.Kill(os.Getpid(), failSig)
	if failSig == syscall.SIGKILL {
		select {}
	}
}
