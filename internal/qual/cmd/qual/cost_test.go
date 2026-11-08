package main

import (
	"math"
	"testing"
)

// TestCostArithmetic checks estimate against a bill computed by hand.
func TestCostArithmetic(t *testing.T) {
	m := map[string]opCost{
		"w":         {PUT: 2, GET: 1, Up: 1000, ObjUp: 500, Down: 100},
		"read-conc": {GET: 1},
		"cold-1m":   {GET: 3, Down: 1.1e6},
		"cold-16m":  {GET: 4.5, Down: 16.1e6}, // => 0.1 GET and 1e6 B per MiB, 2.9 GET and 1e5 B fixed
	}
	p := profile{Entities: 1000, Hot: 0.5, Writes: 0.01, Reads: 0.02, WritePhase: "w", EntityMiB: 2,
		Activations: 0.1, CollectHours: 1, Egress: 1}
	pr := price{StorageGBMonth: 0.02, PutPer1k: 0.005, GetPer1k: 0.0004, EgressGB: 0.1, FreeEgressGB: 10}
	b, err := estimate(p, m, pr)
	if err != nil {
		t.Fatal(err)
	}
	// 5 writes/s, 10 reads/s, 0.1 activations/s (3.1 GET, 2.1e6 B each),
	// 500 entities collected hourly (1 GET + 1 LIST each); 730 h months.
	for _, c := range []struct {
		name      string
		got, want float64
	}{
		{"PUTs", b.PUTs, 10 * month},
		{"LISTs", b.LISTs, 365000},
		{"GETs", b.GETs, (5+10+0.31)*month + 365000},
		{"DownGB", b.DownGB, (5*100 + 0.1*2.1e6) * month / 1e9},
		{"LiveGB", b.LiveGB, 2},
		{"RetainedGB", b.RetainedGB, 5 * 500 * 3600 / 1e9},
		{"Storage", b.Storage, 0.04},
		{"Retained", b.Retained, 0.00018},
		{"PutCost", b.PutCost, (26280000 + 365000) / 1000 * 0.005},
		{"GetCost", b.GetCost, 40599680.0 / 1000 * 0.0004},
		{"Egress", b.Egress, (553.194 - 10) * 0.1},
		{"Total", b.Total, 0.04 + 0.00018 + 133.225 + 16.239872 + 54.3194},
		{"PerMWrites", b.PerMWrites, 10 + 0.4 + 0.01},
		{"PerMReads", b.PerMReads, 0.4},
		{"PerMOps", b.PerMOps, b.Total / (15 * month) * 1e6},
		{"ColdGET", b.Cold.GET, 3.1},
		{"WarmCost", b.WarmCost, 0.1 * month * 0.0004 / 1000},
		{"S3Seconds", b.S3Seconds, (10*putP50 + (15.31+2*500.0/3600)*getP50) * month},
	} {
		if math.Abs(c.got-c.want) > 1e-6*math.Max(1, math.Abs(c.want)) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if b.Dominant != "PUT/LIST requests" {
		t.Errorf("dominant = %q", b.Dominant)
	}
	pr.FreeEgressGB = 1e6 // allowance larger than the traffic: no egress charge
	if b, _ := estimate(p, m, pr); b.Egress != 0 {
		t.Errorf("egress under the free allowance = %v", b.Egress)
	}
}

// TestCostPerOp checks the wire-count conversion of a stress phase.
func TestCostPerOp(t *testing.T) {
	got := perOp(phase{N: 4, Requests: map[string]int{"put-head": 4, "put-object": 2, "get-head": 1, "get-head-304": 2,
		"get-object": 1, "list": 4}, Uploaded: 400, ObjectUploaded: 100, Downloaded: 40})
	if want := (opCost{PUT: 1.5, GET: 1, LIST: 1, Up: 100, ObjUp: 25, Down: 10}); got != want {
		t.Fatalf("perOp = %+v, want %+v", got, want)
	}
	for _, s := range scenarios {
		if _, err := estimate(s, measured, priceTables["scaleway"]); err != nil {
			t.Errorf("%s: %v", s.Name, err)
		}
	}
}
