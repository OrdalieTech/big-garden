package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/OrdalieTech/big-garden/internal/storetest"
)

// faults is the PLAN §16 fault matrix runner (§20.3). Every row names the
// test lanes that cover it; each lane (one go test invocation) runs once, in
// order, at the requested repetitions, and every row it covers gets its
// result. Kill-harness lanes repeat through GARDEN_KILL_RUNS (repetitions
// per failpoint or scenario), the others through -count. A lane passes only
// if go test exits 0, at least one test passed and none was skipped. Local
// lanes run against the pinned S3 fixture (GARDEN_TEST_S3=1); --remote runs
// the remotely reproducible lanes against QUAL_S3_* (GARDEN_TEST_REMOTE=1).
// faults.json holds one result per row and per lane; logs/ holds the output.
func faults(args []string) error {
	fs := flag.NewFlagSet("faults", flag.ContinueOnError)
	remote := fs.Bool("remote", false, "run the remotely reproducible lanes against QUAL_S3_* and AWS_*")
	reps := fs.Int("repetitions", 0, "repetitions per failpoint or test (0: 100 local, 20 remote)")
	rows := fs.String("rows", "", "comma-separated row numbers (empty: every row)")
	out := fs.String("out", "", "output directory (empty: artifacts/local-faults or artifacts/remote-faults)")
	list := fs.Bool("list", false, "print the matrix and exit")
	resume := fs.Bool("resume", false, "keep the passed lanes of --out's faults.json (same mode and repetitions), run the others")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *list {
		for _, r := range matrix {
			fmt.Printf("%2d %-8s %s\n   lanes: %s\n", r.Row, r.Coverage, r.Fault, strings.Join(r.Lanes, " "))
			if r.Gap != "" {
				fmt.Printf("   gap: %s\n", r.Gap)
			}
		}
		return nil
	}
	mode := "local"
	if *remote {
		mode = "remote"
		if _, err := storetest.RemoteFromEnv(); err != nil {
			return err
		}
	}
	if *reps == 0 {
		*reps = map[bool]int{false: 100, true: 20}[*remote]
	}
	if *out == "" {
		*out = filepath.Join("artifacts", mode+"-faults")
	}
	want := map[int]bool{}
	for _, s := range strings.FieldsFunc(*rows, func(r rune) bool { return r == ',' }) {
		n, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("--rows: %v", err)
		}
		want[n] = true
	}
	if err := os.MkdirAll(filepath.Join(*out, "logs"), 0o755); err != nil {
		return err
	}
	root, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return fmt.Errorf("module root: %v", err)
	}
	rep := faultsReport{Mode: mode, Repetitions: *reps, Started: time.Now().UTC()}
	if b, err := exec.Command("git", "-C", strings.TrimSpace(string(root)), "rev-parse", "HEAD").Output(); err == nil {
		rep.Commit = strings.TrimSpace(string(b))
		if st, _ := exec.Command("git", "-C", strings.TrimSpace(string(root)), "status", "--porcelain", "--untracked-files=no").Output(); len(st) > 0 {
			rep.Commit += "-dirty"
		}
	}
	results := map[string]*laneResult{}
	if *resume {
		var prev faultsReport
		if b, err := os.ReadFile(filepath.Join(*out, "faults.json")); err == nil && json.Unmarshal(b, &prev) == nil &&
			prev.Mode == mode && prev.Repetitions == *reps {
			for _, l := range prev.Lanes {
				if l.Status == "pass" {
					results[l.Name] = &l
				}
			}
			rep.Started, rep.Commit = prev.Started, prev.Commit+" resumed at "+rep.Commit
		}
	}
	write := func() error {
		rep.Rows, rep.Lanes, rep.Verdict = nil, nil, "pass"
		for _, r := range matrix {
			if len(want) > 0 && !want[r.Row] {
				continue
			}
			rr := rowResult{Row: r.Row, Fault: r.Fault, Coverage: r.Coverage, Gap: r.Gap, Status: "pass"}
			for _, name := range r.Lanes {
				l := results[name]
				if l == nil || l.Status == "not_remote" {
					continue
				}
				rr.Lanes = append(rr.Lanes, name)
				rr.Tests = append(rr.Tests, l.Tests...)
				rr.Seconds += l.Seconds
				rr.Reps = max(rr.Reps, l.Reps)
				if l.Status != "pass" && rr.Status == "pass" {
					rr.Status = l.Status // pending, fail or skipped
				}
			}
			if len(rr.Lanes) == 0 {
				rr.Status = "not_" + mode
			}
			if rr.Status != "pass" && rr.Status != "not_remote" {
				rep.Verdict = "fail"
			}
			rep.Rows = append(rep.Rows, rr)
		}
		for _, name := range laneOrder {
			if l := results[name]; l != nil {
				rep.Lanes = append(rep.Lanes, *l)
			}
		}
		rep.Finished = time.Now().UTC()
		b, _ := json.MarshalIndent(rep, "", "  ")
		return os.WriteFile(filepath.Join(*out, "faults.json"), append(b, '\n'), 0o644)
	}
	for _, name := range laneOrder {
		if !slices.ContainsFunc(matrix, func(r faultRow) bool { return (len(want) == 0 || want[r.Row]) && slices.Contains(r.Lanes, name) }) {
			continue
		}
		l := lanes[name]
		if results[name] != nil {
			continue // passed before --resume
		}
		res := &laneResult{Name: name, Pkg: l.Pkg, Run: l.Run, Status: "pending"}
		if *remote {
			res.Run = cmp.Or(l.RemoteRun, l.Run)
		}
		results[name] = res
		if *remote && !l.Remote {
			res.Status = "not_remote"
			continue
		}
		write()
		runLane(strings.TrimSpace(string(root)), *out, l, res, *reps, *remote)
		fmt.Printf("%-12s %-7s %6.0fs  %s\n", name, res.Status, res.Seconds, strings.Join(res.Failed, " "))
	}
	if err := write(); err != nil {
		return err
	}
	if rep.Verdict != "pass" {
		return errors.New("fault matrix failed: see " + filepath.Join(*out, "faults.json"))
	}
	return nil
}

// runLane runs one go test invocation with -json and records every test's
// final action.
func runLane(root, out string, l lane, res *laneResult, reps int, remote bool) {
	count := reps
	env := append(os.Environ(), "GARDEN_TEST_S3=1")
	if l.Kill {
		count = 1
		env = append(env, "GARDEN_KILL_RUNS="+strconv.Itoa(reps))
	}
	if l.Count > 0 { // a lane whose single pass already repeats internally
		count = min(count, l.Count)
	}
	if remote {
		env = append(env, "GARDEN_TEST_REMOTE=1")
	}
	res.Reps, res.Env = count, l.Env
	if l.Kill {
		res.Reps = reps
	}
	env = append(env, l.Env...)
	bin, args := "go", []string{"test", "-json", "-count=" + strconv.Itoa(count), "-timeout=0", "-run", res.Run, l.Pkg}
	if l.Qual != nil { // this command itself, e.g. the storage probe
		bin, res.Reps = os.Args[0], 1
		args = append(slices.Clone(l.Qual), "--"+map[bool]string{false: "local", true: "remote"}[remote], "--out="+filepath.Join(out, res.Name))
	}
	res.Command = strings.Join(append([]string{bin}, args...), " ")
	logPath := filepath.Join(out, "logs", res.Name+".log")
	res.Log = logPath
	logf, err := os.Create(logPath)
	if err != nil {
		res.Status, res.Failed = "fail", []string{err.Error()}
		return
	}
	defer logf.Close()
	fmt.Fprintf(logf, "# %s\n# env: %s\n", res.Command, strings.Join(append([]string{"GARDEN_TEST_S3=1"}, l.Env...), " "))
	cmd := exec.CommandContext(context.Background(), bin, args...)
	cmd.Dir, cmd.Env, cmd.Stderr = root, env, logf
	stdout, err := cmd.StdoutPipe()
	start := time.Now()
	if err == nil {
		err = cmd.Start()
	}
	if err != nil {
		res.Status, res.Failed = "fail", []string{err.Error()}
		return
	}
	final := map[string]string{} // test -> pass, fail or skip (any skip or fail of a repetition sticks)
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var ev struct{ Action, Test, Output string }
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			logf.Write(append(sc.Bytes(), '\n'))
			continue
		}
		logf.WriteString(ev.Output)
		if ev.Test == "" || (ev.Action != "pass" && ev.Action != "fail" && ev.Action != "skip") {
			continue
		}
		if final[ev.Test] == "" || final[ev.Test] == "pass" {
			final[ev.Test] = ev.Action
		}
	}
	werr := cmd.Wait()
	res.Seconds = time.Since(start).Seconds()
	passed := 0
	for test, a := range final {
		res.Tests = append(res.Tests, l.Pkg+":"+test)
		switch a {
		case "pass":
			passed++
		case "fail":
			res.Failed = append(res.Failed, test)
		case "skip":
			res.Skipped = append(res.Skipped, test)
		}
	}
	slices.Sort(res.Tests)
	slices.Sort(res.Failed)
	slices.Sort(res.Skipped)
	if l.Qual != nil && werr == nil {
		passed = 1
	}
	switch {
	case werr != nil || len(res.Failed) > 0:
		res.Status = "fail"
		if len(res.Failed) == 0 {
			res.Failed = []string{werr.Error()}
		}
	case len(res.Skipped) > 0 || passed == 0:
		res.Status = "skipped" // never a pass by skipping (PLAN §15.1)
	default:
		res.Status = "pass"
	}
}

type faultsReport struct {
	Mode, Commit, Verdict string
	Repetitions           int
	Started, Finished     time.Time
	Rows                  []rowResult
	Lanes                 []laneResult
}

type rowResult struct {
	Row                  int
	Fault, Coverage, Gap string `json:",omitempty"`
	Status               string
	Lanes, Tests         []string
	Reps                 int
	Seconds              float64
}

type laneResult struct {
	Name, Pkg, Run, Command, Log, Status string
	Env                                  []string `json:",omitempty"`
	Reps                                 int
	Seconds                              float64
	Tests, Failed, Skipped               []string `json:",omitempty"`
}

// lane is one go test invocation.
type lane struct {
	Pkg, Run  string
	RemoteRun string   // -run with --remote, if narrower
	Kill      bool     // repetitions through GARDEN_KILL_RUNS, else -count
	Count     int      // cap on -count (0: the repetitions)
	Remote    bool     // reproducible against a remote store
	Env       []string // extra environment
	Qual      []string // instead of go test: this command with these arguments (+ --local/--remote, --out)
}

const kn = "./internal/qual/killnode"

var lanes = map[string]lane{
	// subprocess SIGKILL/SIGSTOP harness on the S3 fixture
	"kill":       {Pkg: kn, Run: "^TestProcessKill$", RemoteRun: "^TestProcessKill$/^(sql|capture|put|cas|reply)", Kill: true, Remote: true},
	"batchkill":  {Pkg: kn, Run: "^TestBatchKill$", Kill: true, Remote: true},
	"staleowner": {Pkg: kn, Run: "^TestStaleOwner$", Kill: true, Remote: true},
	"takeover":   {Pkg: kn, Run: "^TestConcurrentTakeover$", Kill: true, Remote: true},
	"takeover1":  {Pkg: kn, Run: "^TestConcurrentTakeover$", Kill: true, Remote: true, Env: []string{"GARDEN_TEST_BATCH=1"}},
	"sameid":     {Pkg: kn, Run: "^TestSameIDSeveralNodes$", Kill: true, Remote: true},
	"routed":     {Pkg: kn, Run: "^(TestLinearizabilityChecker|TestRoutedHistories)$", Kill: true, Remote: true},
	"stalemaint": {Pkg: kn, Run: "^TestStaleMaintenance$", Kill: true, Remote: true},
	"warmkill":   {Pkg: kn, Run: "^TestWarmKill$", Kill: true, Remote: true},
	"diskfull":   {Pkg: kn, Run: "^TestDiskFull$", Kill: true, Remote: true},
	"deletekill": {Pkg: kn, Run: "^TestDeleteKill$", Kill: true, Remote: true},
	// fault-scripted core tests: S3 adapter through a fault proxy on the fixture (Mem for the Mem-only ones)
	"publication": {Pkg: ".", Run: "^Test(ResolveBranches|ResolvePrunedReceipt|ResolveObjectBranch|ResolveHydrationFailureIsUnknown|RestartRecoversEverything|ReproposeAfterRecovery|DeadlineEndsWaiting)$", Count: 20, Remote: true},
	"create":      {Pkg: ".", Run: "^Test(ConcurrentCreates|ConcurrentSameCreate|CreateIdempotentAndConflicts|CreateResumeD5|CreateAfterBirthIsUnknown|IDExpiredByFloor)$", Count: 20, Remote: true},
	"firstopen":   {Pkg: ".", Run: "^TestConcurrentFirstOpen$", Count: 100},
	"batch":       {Pkg: ".", Run: "^TestBatch(Members|NeverPassesRead|Unresolved|Cancellation|HotLedger)$", Count: 20, Remote: true},
	"ownership":   {Pkg: ".", Run: "^Test(OwnershipAndMigrate|CreateUpdateReadLookup|SchemaMismatchReportsStored|NewerHeadFormatRefused)$", Count: 20, Remote: true},
	"maintenance": {Pkg: ".", Run: "^Test(StaleMaintenanceAcrossTakeover|CollectPausedAfterHead|CollectedObjectReuploaded|CollectDuringActivation)$", Count: 20, Remote: true},
	"corrupt":     {Pkg: ".", Run: "^Test(CorruptRetainedObjects|CorruptBirthAndNamespace|ImportRejectsMalformedArchives)$", Count: 20},
	"missing":     {Pkg: ".", Run: "^Test(MissingHeadFailsClosed|OpenNamespace)$", Count: 20, Remote: true},
	"import":      {Pkg: ".", Run: "^Test(Import|ImportWhileOldWritersWrite)$", Count: 20, Remote: true},
	"bounds":      {Pkg: ".", Run: "^Test(PendingPerEntity|InflightStorageBound|RecoveriesBound|CacheBytes|EntityMapBounded)$", Count: 20, Remote: true},
	"extend":      {Pkg: ".", Run: "^TestExtendBoundary$", Count: 20},
	"delete":      {Pkg: ".", Run: "^TestDelete(|Follower|ReviewCases)$", Count: 20, Remote: true},
	"reads":       {Pkg: ".", Run: "^Test(SharedValidation|ReadRidesCAS|FollowerRead|ExportFollower|HundredReaders)$", Count: 20, Remote: true},
	"warm":        {Pkg: ".", Run: "^TestWarm(Tier|Budget|KeepsFileOnStoreError)$", Count: 20},
	"outage":      {Pkg: ".", Run: "^Test(OutageFailsFast|ClockGuard|StaleHeadRefused|ResidentBoundUnderConcurrentCreates)$", Count: 20},
	"format1":     {Pkg: ".", Run: "^TestFormat1Frozen$", Count: 5},
	// storage adapter against an HTTP front-end over Mem and the fixture
	"store": {Pkg: "./internal/store", Run: "^TestS3(Classification|TransportFaults|Hedge|RetryStopsAtDeadline)$", Count: 20, Remote: true},
	// no store: SQLite capture, restore and the SQL boundary
	"restore":    {Pkg: "./internal/engine", Run: "^Test(RestoreRejects|RestoreOpensWithoutWriting)$", Count: 20},
	"capture":    {Pkg: "./internal/engine", Run: "^Test(OracleRandomized|AutoRollbackCannotCommit|MutateReceiptsAndRollback)$", Count: 5},
	"wal":        {Pkg: "./internal/qual/m0", Run: "^Test(DBPageCapture|CloseCheckpointsWAL|LTXRejects|LTXDeclaredSizeAllocation)$", Count: 20},
	"authorizer": {Pkg: "./internal/engine", Run: "^Test(AuthorizerBypass|ScopeRace|RenameIntoReserved|ForeignKeyToReserved)$", Count: 20},
	"authconn":   {Pkg: "./internal/qual/m0", Run: "^TestAuthorizerPerConnection$", Count: 20},
	"head":       {Pkg: "./internal/head", Run: "^Test(ValidateRejects|DecodeRejectsNonCanonical|DecodeRejectsNonCanonicalTail|NamespaceAndBirth|Tombstone)$", Count: 20},
	"layout":     {Pkg: "./internal/engine", Run: "^Test(OldReceiptsLayoutRejected|SecureDelete|ReceiptCost)$", Count: 5},
	// the storage probe: PLAN §16's 1,000 contested rounds with 32 contenders, and its negative controls
	"doctor":    {Qual: []string{"doctor", "--cas-rounds=1000", "--contenders=32"}, Remote: true},
	"doctorneg": {Pkg: "./internal/qual/cmd/qual", Run: "^(TestDoctorRejectsStoreIgnoringConditions|TestDoctorPassesLinearizableStore)$", Count: 5},
}

// laneOrder runs the cheap lanes first.
var laneOrder = []string{"head", "authorizer", "authconn", "restore", "capture", "wal", "store", "doctorneg", "doctor",
	"firstopen", "corrupt", "extend", "missing", "create", "publication", "batch", "ownership", "maintenance", "import", "bounds",
	"layout", "format1", "warm", "outage", "reads", "delete",
	"kill", "batchkill", "staleowner", "takeover", "takeover1", "sameid", "warmkill", "diskfull", "deletekill", "routed", "stalemaint"}

type faultRow struct {
	Row             int
	Fault, Coverage string
	Lanes           []string
	Gap             string
}

// matrix is PLAN §16, row by row, with the lanes that cover each row and
// what they do not (Coverage "partial").
var matrix = []faultRow{
	{1, "Kill before/inside SQL transaction, after local COMMIT, during LTX capture", "covered", []string{"kill", "batchkill", "warmkill"}, ""},
	{2, "Kill before PUT, mid-body, after object application but before response", "covered", []string{"kill", "batchkill", "store"}, "put.mid kills 0-3 ms after the PUT starts, not provably mid-body"},
	{3, "Kill before head CAS, after CAS application, after response but before acknowledgment", "covered", []string{"kill", "batchkill", "deletekill"}, ""},
	{4, "Kill after observed success and delete every node/cache disk", "covered", []string{"kill", "routed", "publication", "warm"}, ""},
	{5, "Store applies a request after its client deadline or disconnect", "covered", []string{"publication", "store", "kill"}, "net.late (kill lane) is local only"},
	{6, "CAS succeeds, reply lost, SDK would retry and receive 412", "covered", []string{"store", "publication", "kill"}, ""},
	{7, "Two/32 simultaneous create or takeover candidates", "partial", []string{"takeover", "takeover1", "create", "firstopen", "doctor", "doctorneg"}, "entity-level create races 8 contenders on 2 nodes; 32-way create is the storage probe (qual doctor --cas-rounds=1000 --contenders=32)"},
	{8, "SIGSTOP owner, take over elsewhere, commit, SIGCONT old owner", "covered", []string{"staleowner", "takeover", "takeover1", "sameid", "routed"}, ""},
	{9, "Old commit wins immediately before takeover", "partial", []string{"staleowner", "takeover", "takeover1"}, "a commit landing between the takeover's GET and its claim is reached by random timing, not forced"},
	{10, "Same operation ID on several nodes with same/different hashes", "covered", []string{"sameid", "kill", "create", "batch"}, ""},
	{11, "Receipt pruning races delayed retries; clocks jump ±1 hour", "partial", []string{"publication", "create", "outage"}, "IDs ±1-2 h from the node clock; no jump of the owner's own clock"},
	{12, "Read begins after a completed write on another owner", "covered", []string{"routed", "staleowner", "ownership", "reads", "delete"}, ""},
	{13, "Read/error follows an unpublished earlier batch member", "covered", []string{"batch", "reads"}, ""},
	{14, "Context canceled mid-batch; one callback errors; another succeeds", "covered", []string{"batch", "batchkill"}, ""},
	{15, "Compactor paused before/after upload or CAS across takeover", "covered", []string{"maintenance", "stalemaint"}, ""},
	{16, "Collector pauses after reading head, ownership changes, new objects publish", "covered", []string{"maintenance", "delete", "deletekill"}, ""},
	{17, "Stale writer uploads after its orphan was deleted", "covered", []string{"maintenance"}, ""},
	{18, "Restore/export races collection of an older root", "partial", []string{"maintenance", "delete"}, "activation racing Collect and Delete; Export racing Collect is not scheduled"},
	{19, "Header/length/digest/checksum corrupted; LTX duplicated/reordered; referenced object missing", "covered", []string{"corrupt", "restore", "head", "store", "import", "warm"}, ""},
	{20, "Compressed object expands beyond configured limits", "partial", []string{"restore", "wal", "corrupt"}, "declared-size and frame-size rejections; allocation during decompression is not measured"},
	{21, "WAL partial header/frame, wrong salt/checksum, uncommitted tail, duplicate pages, shrink/regrow", "partial", []string{"capture", "wal", "restore"}, "no WAL parser (DECISIONS D2): capture reads committed pages through SQLite; rollback, shrink/regrow covered, torn WAL injection not applicable"},
	{22, "SQL caller attempts COMMIT, ATTACH, storage PRAGMA, reserved-table write, escaping cursor", "covered", []string{"authorizer", "authconn", "extend"}, ""},
	{23, "ENOSPC/EIO during WAL, staging, checkpoint, replay, rename or export", "partial", []string{"diskfull"}, "real EFBIG write errors under RLIMIT_FSIZE on node processes (WAL, staging, checkpoint, replay); EIO and export to a failing writer are not injected"},
	{24, "Head missing beside birth/data; namespace descriptor missing beside existing data", "covered", []string{"missing", "create", "corrupt"}, ""},
	{25, "Wrong incarnation/namespace/key, unknown codec/schema, downgraded binary", "partial", []string{"ownership", "head", "restore", "import", "format1", "layout"}, "a newer-format head is refused by every path before any write (TestNewerHeadFormatRefused); no older-binary process run"},
	{26, "Import old backup while old nodes still run", "covered", []string{"import"}, ""},
	{27, "Store ignores conditions or serves provably stale overwritten heads", "partial", []string{"missing", "doctor", "doctorneg", "outage"}, "condition-ignoring store rejected (doctor negative control, Open probe); no stale-read negative control"},
	{28, "S3 unavailable for 60 seconds; all queues reach limits", "partial", []string{"bounds", "publication", "batch", "outage"}, "short outages with bounded queues; the 60 s outage runs in qual soak only"},
	{29, "Repeated failovers and node replacement under load", "partial", []string{"routed"}, "light load in the test; sustained failover under load is qual soak"},
}
