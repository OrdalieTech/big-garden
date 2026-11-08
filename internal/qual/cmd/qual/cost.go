package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

// cost turns measured per-operation S3 traffic into a monthly bill and S3
// time (PLAN §19): a workload profile (built-in scenarios or flags), the
// per-operation counts of one `qual stress` run (built-in: the 2026-11-08
// local RustFS run in docs/COST.md, or --stress stress.json) and a dated
// price table (built-in, overridable by flags).
func cost(args []string) error {
	fs := flag.NewFlagSet("cost", flag.ContinueOnError)
	stressFile := fs.String("stress", "", "stress.json of `qual stress --out` (default: built-in measured counts)")
	prices := fs.String("prices", "scaleway", "price table: "+strings.Join(priceNames(), ", "))
	scenario := fs.String("scenario", "all", "built-in scenario or all; ignored when --entities is set")
	asJSON := fs.Bool("json", false, "print JSON instead of text")
	var p profile
	fs.Float64Var(&p.Entities, "entities", 0, "custom profile: retained entities")
	fs.Float64Var(&p.Hot, "hot", 1, "custom profile: fraction of entities active")
	fs.Float64Var(&p.Writes, "writes", 0, "custom profile: mutations/s per active entity")
	fs.Float64Var(&p.Reads, "reads", 0, "custom profile: strong reads/s per active entity")
	fs.StringVar(&p.WritePhase, "write-phase", "inline-seq", "stress phase whose per-mutation counts model writes (inline-conc, hot-8, hot-32, upload-seq, ...)")
	fs.Float64Var(&p.EntityMiB, "entity-mib", 1, "custom profile: entity image MiB")
	fs.Float64Var(&p.Activations, "activations", 0, "custom profile: cold activations/s, all entities")
	fs.Float64Var(&p.CollectHours, "collect-hours", 24, "custom profile: Collect interval per written entity (superseded objects stay stored until then)")
	fs.Float64Var(&p.Egress, "egress", 0, "billable fraction of downloaded bytes (0 = compute in the store's region); also applies to built-in scenarios")
	over := map[string]*float64{}
	for _, f := range []string{"storage-price", "put-price", "get-price", "egress-price", "free-egress"} {
		over[f] = fs.Float64("p-"+f, -1, f+" override (per GB-month, per 1000 requests, per GB, GB/month)")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	pr, ok := priceTables[*prices]
	if !ok {
		return fmt.Errorf("unknown price table %q", *prices)
	}
	for f, dst := range map[string]*float64{"storage-price": &pr.StorageGBMonth, "put-price": &pr.PutPer1k,
		"get-price": &pr.GetPer1k, "egress-price": &pr.EgressGB, "free-egress": &pr.FreeEgressGB} {
		if *over[f] >= 0 {
			*dst = *over[f]
		}
	}
	m := measured
	if *stressFile != "" {
		b, err := os.ReadFile(*stressFile)
		if err != nil {
			return err
		}
		var r stressReport
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}
		m = map[string]opCost{}
		for _, ph := range r.Phases {
			m[ph.Name] = perOp(ph)
		}
	}
	var runs []profile
	switch {
	case p.Entities > 0:
		p.Name = "custom"
		runs = []profile{p}
	case *scenario == "all":
		runs = scenarios
	default:
		i := slices.IndexFunc(scenarios, func(s profile) bool { return s.Name == *scenario })
		if i < 0 {
			return fmt.Errorf("unknown scenario %q", *scenario)
		}
		runs = scenarios[i : i+1]
	}
	runs = slices.Clone(runs)
	for i := range runs {
		runs[i].Egress = max(runs[i].Egress, p.Egress)
	}
	var out []bill
	for _, s := range runs {
		b, err := estimate(s, m, pr)
		if err != nil {
			return err
		}
		out = append(out, b)
	}
	if *asJSON {
		e := json.NewEncoder(os.Stdout)
		e.SetIndent("", "  ")
		return e.Encode(out)
	}
	for _, b := range out {
		b.print(os.Stdout)
	}
	return nil
}

// opCost is the physical S3 traffic of one logical operation.
type opCost struct{ PUT, GET, LIST, Up, ObjUp, Down float64 }

func (o opCost) plus(p opCost, k float64) opCost {
	return opCost{o.PUT + k*p.PUT, o.GET + k*p.GET, o.LIST + k*p.LIST, o.Up + k*p.Up, o.ObjUp + k*p.ObjUp, o.Down + k*p.Down}
}

func (o opCost) per(n float64) opCost { return opCost{}.plus(o, 1/n) }

// perOp divides a stress phase's wire counts (maintenance included) by its
// operations.
func perOp(ph phase) opCost {
	n := float64(ph.N)
	r := ph.Requests
	return opCost{PUT: float64(r["put-head"]+r["put-object"]) / n,
		GET: float64(r["get-head"]+r["get-head-304"]+r["get-object"]) / n, LIST: float64(r["list"]) / n,
		Up: float64(ph.Uploaded) / n, ObjUp: float64(ph.ObjectUploaded) / n, Down: float64(ph.Downloaded) / n}
}

// measured: `qual stress --local --ops 400 --workers 8 --cold 10 --batch 16
// --hot 1,8,32` against the pinned RustFS fixture, 2026-11-08 (docs/COST.md).
// Totals per phase, divided by the phase's operation count.
var measured = map[string]opCost{
	"create":      opCost{PUT: 24, GET: 16, LIST: 8, Up: 16808, ObjUp: 9840, Down: 4752}.per(8),
	"inline-seq":  opCost{PUT: 428, GET: 24, Up: 12093464, ObjUp: 999851, Down: 843699}.per(400),
	"inline-conc": opCost{PUT: 410, Up: 8324374, ObjUp: 180963}.per(400),
	"hot-8":       opCost{PUT: 77, GET: 24, Up: 2095282, ObjUp: 1054027, Down: 865050}.per(400),
	"hot-32":      opCost{PUT: 46, GET: 8, Up: 1314353, ObjUp: 948007, Down: 316160}.per(400),
	"upload-seq":  opCost{PUT: 228, GET: 88, Up: 12144140, ObjUp: 11731960, Down: 4884218}.per(100),
	"read-conc":   opCost{GET: 400}.per(400), // every head GET answered 304
	"cold-1m":     opCost{GET: 40, Down: 10656170}.per(10),
	"cold-16m":    opCost{GET: 60, Down: 169953540}.per(10),
}

// profile is a steady-state workload.
type profile struct {
	Name, Note             string
	Entities, Hot          float64 // retained entities; active fraction
	Writes, Reads          float64 // per active entity per second
	WritePhase             string  // stress phase modeling one mutation
	EntityMiB, Activations float64 // image size; cold activations/s
	CollectHours, Egress   float64
}

var scenarios = []profile{
	{Name: "small", Note: "1k entities x 1 write/min + 1 read/min, all active", Entities: 1000, Hot: 1,
		Writes: 1.0 / 60, Reads: 1.0 / 60, WritePhase: "inline-seq", EntityMiB: 1, Activations: 0.01, CollectHours: 24},
	{Name: "fleet", Note: "10k entities, hot 5% at 1 write/s + 1 read/s each", Entities: 10000, Hot: 0.05,
		Writes: 1, Reads: 1, WritePhase: "inline-seq", EntityMiB: 1, Activations: 1, CollectHours: 24},
	{Name: "hot", Note: "100 entities x 50 writes/s, queued writers batch (B=8 measured)", Entities: 100, Hot: 1,
		Writes: 50, Reads: 5, WritePhase: "hot-8", EntityMiB: 1, Activations: 0, CollectHours: 1},
	{Name: "read-heavy", Note: "10k entities, hot 10%, 70% reads / 30% writes, 1 op/s each", Entities: 10000, Hot: 0.1,
		Writes: 0.3, Reads: 0.7, WritePhase: "inline-seq", EntityMiB: 1, Activations: 2, CollectHours: 24},
}

// price is a dated provider price table, excluding VAT.
type price struct {
	Name, Currency, Source             string
	StorageGBMonth, PutPer1k, GetPer1k float64 // decimal GB; PUT also prices LIST
	EgressGB, FreeEgressGB             float64
}

// Fetched 2026-11-08; docs/COST.md lists the sources.
var priceTables = map[string]price{
	"scaleway": {Name: "scaleway fr-par Standard Multi-AZ", Currency: "EUR", StorageGBMonth: 0.01606,
		EgressGB: 0.01, FreeEgressGB: 75, Source: "scaleway.com/en/pricing/storage + public product catalog"},
	"scaleway-onezone": {Name: "scaleway fr-par Standard One Zone", Currency: "EUR", StorageGBMonth: 0.00803,
		EgressGB: 0.01, FreeEgressGB: 75, Source: "scaleway.com/en/pricing/storage + public product catalog"},
	"aws-us-east-1": {Name: "aws us-east-1 S3 Standard", Currency: "USD", StorageGBMonth: 0.023, PutPer1k: 0.005,
		GetPer1k: 0.0004, EgressGB: 0.09, FreeEgressGB: 100, Source: "AWS Price List API AmazonS3 + AWSDataTransfer"},
	"aws-eu-west-3": {Name: "aws eu-west-3 S3 Standard", Currency: "USD", StorageGBMonth: 0.024, PutPer1k: 0.0053,
		GetPer1k: 0.00042, EgressGB: 0.09, FreeEgressGB: 100, Source: "AWS Price List API AmazonS3 + AWSDataTransfer"},
	"hypothetical": {Name: "PLAN 19.2 hypothetical paid requests", Currency: "EUR", StorageGBMonth: 0.03,
		PutPer1k: 0.005, GetPer1k: 0.0004, EgressGB: 0.09, Source: "PLAN 19.2 (not a quote)"},
}

func priceNames() []string {
	var s []string
	for k := range priceTables {
		s = append(s, k)
	}
	slices.Sort(s)
	return s
}

const (
	month = 730 * 3600.0 // seconds; providers bill 730 h/month
	gb    = 1e9
	mib   = 1 << 20
)

// bill is the monthly cost of one profile under one price table.
type bill struct {
	Profile                                     profile
	Prices                                      string
	WritesPerSec, ReadsPerSec, ActsPerSec       float64
	PUTs, GETs, LISTs, UpGB, DownGB             float64 // per month
	LiveGB, RetainedGB                          float64
	Storage, Retained, PutCost, GetCost, Egress float64 // per month, table currency
	Total, PerMOps, PerMWrites, PerMReads       float64
	Dominant                                    string
	Cold, Warm, WarmDeltas                      opCost  // one reactivation
	ColdCost, WarmCost                          float64 // per month at ActsPerSec
	S3Seconds                                   float64 // request-seconds per month at p50
	Currency                                    string
}

// p50 S3 request latency on Scaleway fr-par from one host (docs/PERFORMANCE.md).
const putP50, getP50 = 0.035, 0.009

func estimate(p profile, m map[string]opCost, pr price) (bill, error) {
	w, okw := m[p.WritePhase]
	r, okr := m["read-conc"]
	c1, ok1 := m["cold-1m"]
	c16, ok16 := m["cold-16m"]
	if !okw || !okr || !ok1 || !ok16 {
		return bill{}, errors.New("counts need phases " + p.WritePhase + ", read-conc, cold-1m and cold-16m")
	}
	// Activation = fixed part + per-MiB part, fitted on the 1 and 16 MiB runs.
	perMiB := c16.plus(c1, -1)
	perMiB = opCost{}.plus(perMiB, 1.0/15)
	base := c1.plus(perMiB, -1)
	cold := base.plus(perMiB, p.EntityMiB)
	b := bill{Profile: p, Prices: pr.Name, Currency: pr.Currency, Cold: cold,
		Warm:       opCost{GET: 1},             // one conditional head GET, 304
		WarmDeltas: opCost{GET: 2, Down: 64e3}, // head 200 with a full inline tail + one compacted catch-up delta
	}
	active := p.Entities * p.Hot
	b.WritesPerSec, b.ReadsPerSec, b.ActsPerSec = active*p.Writes, active*p.Reads, p.Activations
	var t opCost
	t = t.plus(w, b.WritesPerSec).plus(r, b.ReadsPerSec).plus(cold, b.ActsPerSec)
	if p.CollectHours > 0 && p.Writes > 0 { // one head GET + one LIST page per collection
		t = t.plus(opCost{GET: 1, LIST: 1}, active/(p.CollectHours*3600))
	}
	b.PUTs, b.GETs, b.LISTs = t.PUT*month, t.GET*month, t.LIST*month
	b.UpGB, b.DownGB = t.Up*month/gb, t.Down*month/gb
	b.LiveGB = p.Entities * p.EntityMiB * perMiB.Down / gb // stored bytes ~ bytes a restore reads
	b.RetainedGB = b.WritesPerSec * w.ObjUp * p.CollectHours * 3600 / gb
	b.Storage, b.Retained = b.LiveGB*pr.StorageGBMonth, b.RetainedGB*pr.StorageGBMonth
	b.PutCost = (b.PUTs + b.LISTs) / 1000 * pr.PutPer1k
	b.GetCost = b.GETs / 1000 * pr.GetPer1k
	b.Egress = max(0, b.DownGB*p.Egress-pr.FreeEgressGB) * pr.EgressGB
	b.Total = b.Storage + b.Retained + b.PutCost + b.GetCost + b.Egress
	if ops := (b.WritesPerSec + b.ReadsPerSec) * month; ops > 0 {
		b.PerMOps = b.Total / ops * 1e6
	}
	if b.WritesPerSec > 0 {
		b.PerMWrites = (w.PUT*pr.PutPer1k/1000 + w.GET*pr.GetPer1k/1000 + w.Down*p.Egress*pr.EgressGB/gb) * 1e6
	}
	if b.ReadsPerSec > 0 {
		b.PerMReads = (r.GET*pr.GetPer1k/1000 + r.Down*p.Egress*pr.EgressGB/gb) * 1e6
	}
	reqCost := func(o opCost) float64 {
		return (o.PUT+o.LIST)*pr.PutPer1k/1000 + o.GET*pr.GetPer1k/1000 + o.Down*p.Egress*pr.EgressGB/gb
	}
	b.ColdCost, b.WarmCost = reqCost(cold)*b.ActsPerSec*month, reqCost(b.Warm)*b.ActsPerSec*month
	b.S3Seconds = (t.PUT*putP50 + (t.GET+t.LIST)*getP50) * month
	parts := map[string]float64{"storage": b.Storage + b.Retained, "PUT/LIST requests": b.PutCost, "GET requests": b.GetCost, "egress": b.Egress}
	for k, v := range parts {
		if b.Dominant == "" || v > parts[b.Dominant] || (v == parts[b.Dominant] && k < b.Dominant) {
			b.Dominant = k
		}
	}
	return b, nil
}

func (b bill) print(w io.Writer) {
	p := b.Profile
	c := b.Currency
	fmt.Fprintf(w, "%s (%s) — prices %s\n", p.Name, p.Note, b.Prices)
	fmt.Fprintf(w, "  load: %.4g writes/s, %.4g reads/s, %.4g cold activations/s of %.4g MiB; collect every %gh\n",
		b.WritesPerSec, b.ReadsPerSec, b.ActsPerSec, p.EntityMiB, p.CollectHours)
	fmt.Fprintf(w, "  traffic/month: %.4g PUT, %.4g GET, %.4g LIST, %.4g GB up, %.4g GB down (billable %.0f%%)\n",
		b.PUTs, b.GETs, b.LISTs, b.UpGB, b.DownGB, 100*p.Egress)
	fmt.Fprintf(w, "  storage  live %.4g GB %s %.2f | retained %.4g GB %s %.2f\n", b.LiveGB, c, b.Storage, b.RetainedGB, c, b.Retained)
	fmt.Fprintf(w, "  requests PUT+LIST %s %.2f | GET %s %.2f | egress %s %.2f\n", c, b.PutCost, c, b.GetCost, c, b.Egress)
	fmt.Fprintf(w, "  total    %s %.2f/month (dominant: %s); %s %.4f per M logical ops, %.4f per M writes, %.4f per M reads (requests+egress)\n",
		c, b.Total, b.Dominant, c, b.PerMOps, b.PerMWrites, b.PerMReads)
	fmt.Fprintf(w, "  reactivation: cold %.1f GET %.0f KB; warm unchanged 1 GET (304) 0 KB; warm catch-up %.0f GET %.0f KB; activations %s %.2f/month cold vs %s %.2f warm\n",
		b.Cold.GET, b.Cold.Down/1e3, b.WarmDeltas.GET, b.WarmDeltas.Down/1e3, c, b.ColdCost, c, b.WarmCost)
	fmt.Fprintf(w, "  S3 time: %.4g request-seconds/month at p50 = %.1f requests in flight on average\n\n", b.S3Seconds, b.S3Seconds/month)
}
