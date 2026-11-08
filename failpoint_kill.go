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
// the request that follows is in flight.
var failName, failHits = func() (string, int64) {
	name, n, _ := strings.Cut(os.Getenv("GARDEN_FAILPOINT"), ":")
	hits, _ := strconv.ParseInt(n, 10, 64)
	return name, max(hits, 1)
}()

var failCount atomic.Int64

func failpoint(name string) {
	if name != failName || failCount.Add(1) != failHits {
		return
	}
	if strings.HasSuffix(name, ".mid") {
		go func() {
			time.Sleep(rand.N(3 * time.Millisecond))
			syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}()
		return
	}
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}
