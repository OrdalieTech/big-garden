package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/scanner"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// footprint measures the PLAN §12 budgets: production files and lines of the
// runtime-reachable project packages, the exported API (snapshot diffed
// against the committed -api file), the runtime module closure with licenses, native
// artifacts, branchy functions, and minimal consumer binary sizes.
func footprint(args []string) error {
	fs := flag.NewFlagSet("footprint", flag.ContinueOnError)
	maxFiles := fs.Int("max-production-files", 25, "")
	maxLOC := fs.Int("max-production-loc", 6000, "")
	maxDirect := fs.Int("max-direct-modules", 5, "")
	maxRuntime := fs.Int("max-runtime-modules", 30, "")
	maxTypes := fs.Int("max-exported-types", 18, "")
	maxCallables := fs.Int("max-exported-callables", 30, "")
	maxBinary := fs.Int64("max-binary-mib", 35, "stripped consumer binary target")
	apiPath := fs.String("api", "internal/qual/cmd/qual/testdata/api.txt", "committed API snapshot to diff against")
	updateAPI := fs.Bool("update-api", false, "accept the current API as the new snapshot")
	out := fs.String("out", "artifacts/footprint", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	type pkg struct {
		ImportPath, Dir                      string
		GoFiles, CgoFiles, CFiles, SysoFiles []string
		Imports                              []string
		Module                               *struct{ Path, Version, Dir string }
		Standard                             bool
	}
	raw, err := exec.Command("go", "list", "-deps", "-json", "./internal/qual/consumer", "./vec").Output()
	if err != nil {
		return fmt.Errorf("go list: %v", err)
	}
	var pkgs []pkg
	for d := json.NewDecoder(bytes.NewReader(raw)); d.More(); {
		var p pkg
		if err := d.Decode(&p); err != nil {
			return err
		}
		pkgs = append(pkgs, p)
	}
	const self = "github.com/OrdalieTech/big-garden"
	r := map[string]any{}
	var fails []string
	check := func(name string, v, max int64) {
		r[name] = map[string]int64{"value": v, "max": max}
		if v > max {
			fails = append(fails, fmt.Sprintf("%s %d > %d", name, v, max))
		}
	}
	files, loc := 0, 0
	perPkg := map[string]int{}
	var branchy []string
	byMod := map[string]*struct{ Path, Version, Dir string }{}
	direct := map[string]bool{}
	var native []string
	for _, p := range pkgs {
		if p.Standard || p.Module == nil {
			continue
		}
		if len(p.CgoFiles)+len(p.CFiles)+len(p.SysoFiles) > 0 {
			native = append(native, p.ImportPath)
		}
		if p.Module.Path != self {
			byMod[p.Module.Path] = p.Module
			continue
		}
		if strings.HasPrefix(p.ImportPath, self+"/internal/qual") {
			continue // the consumer itself is a measuring tool, not production
		}
		for _, imp := range p.Imports {
			for _, q := range pkgs {
				if q.ImportPath == imp && q.Module != nil && q.Module.Path != self {
					direct[q.Module.Path] = true
				}
			}
		}
		for _, f := range p.GoFiles {
			n, b, err := codeLines(filepath.Join(p.Dir, f))
			if err != nil {
				return err
			}
			files++
			loc += n
			perPkg[strings.TrimPrefix(p.ImportPath, self)] += n
			for _, fn := range b {
				branchy = append(branchy, strings.TrimPrefix(p.ImportPath, self)+"/"+f+": "+fn)
			}
		}
	}
	check("production_files", int64(files), int64(*maxFiles))
	check("production_loc", int64(loc), int64(*maxLOC))
	check("direct_modules", int64(len(direct)), int64(*maxDirect))
	check("runtime_modules", int64(len(byMod)), int64(*maxRuntime))
	r["loc_by_package"], r["functions_over_20_branches"], r["native_packages"] = perPkg, branchy, native
	if len(native) > 0 {
		fails = append(fails, "native code in the consumer closure: "+strings.Join(native, ", "))
	}
	mods := []map[string]string{}
	for _, m := range byMod {
		mods = append(mods, map[string]string{"module": m.Path, "version": m.Version, "license": license(m.Dir), "direct": fmt.Sprint(direct[m.Path])})
	}
	slices.SortFunc(mods, func(a, b map[string]string) int { return strings.Compare(a["module"], b["module"]) })
	r["modules"] = mods

	api, types, callables, err := apiSnapshot(".")
	if err != nil {
		return err
	}
	check("exported_types", int64(types), int64(*maxTypes))
	check("exported_callables", int64(callables), int64(*maxCallables))
	os.MkdirAll(*out, 0o755)
	if old, err := os.ReadFile(*apiPath); err == nil && string(old) != api && !*updateAPI {
		fails = append(fails, "public API changed against "+*apiPath+" (rerun with -update-api to accept)")
	} else if err := os.WriteFile(*apiPath, []byte(api), 0o644); err != nil {
		return err
	}

	sizes := map[string]int64{}
	for _, arch := range []string{"amd64", "arm64"} {
		for _, strip := range []bool{true, false} {
			bin := filepath.Join(os.TempDir(), "qual-consumer-"+arch)
			ld := "-buildid="
			if strip {
				ld = "-s -w -buildid="
			}
			cmd := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-ldflags="+ld, "-o", bin, "./internal/qual/consumer")
			cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+arch, "CGO_ENABLED=0")
			if b, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("build %s: %v: %s", arch, err, b)
			}
			st, _ := os.Stat(bin)
			os.Remove(bin)
			k := "consumer_" + arch
			if !strip {
				k += "_unstripped"
			} else if st.Size() > *maxBinary<<20 {
				fails = append(fails, fmt.Sprintf("%s %d bytes > %d MiB", k, st.Size(), *maxBinary))
			}
			sizes[k] = st.Size()
		}
	}
	r["binary_bytes"] = sizes
	r["verdict"], r["failures"] = "pass", fails
	if len(fails) > 0 {
		r["verdict"] = "fail"
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, "footprint.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("%s\n", b)
	if len(fails) > 0 {
		return fmt.Errorf("%s", strings.Join(fails, "; "))
	}
	return nil
}

// codeLines counts the lines of path holding a token other than a comment,
// and names the functions with more than 20 branch points.
func codeLines(path string) (int, []string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}
	fset := token.NewFileSet()
	var s scanner.Scanner
	s.Init(fset.AddFile(path, -1, len(src)), src, nil, 0)
	lines := map[int]bool{}
	for {
		pos, tok, _ := s.Scan()
		if tok == token.EOF {
			break
		}
		lines[fset.Position(pos).Line] = true
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		return 0, nil, err
	}
	var branchy []string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		n := 0
		ast.Inspect(fn.Body, func(x ast.Node) bool {
			switch x := x.(type) {
			case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.CaseClause, *ast.CommClause:
				n++
			case *ast.BinaryExpr:
				if x.Op == token.LAND || x.Op == token.LOR {
					n++
				}
			}
			return true
		})
		if n > 20 {
			branchy = append(branchy, fmt.Sprintf("%s (%d)", fn.Name.Name, n))
		}
	}
	return len(lines), branchy, nil
}

// apiSnapshot prints the exported declarations of the package in dir, without
// bodies or comments, sorted: the text CI diffs to catch API changes.
func apiSnapshot(dir string) (string, int, int, error) {
	fset := token.NewFileSet()
	ps, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		return "", 0, 0, err
	}
	var decls []string
	types, callables := 0, 0
	print := func(n any) string {
		var b bytes.Buffer
		printer.Fprint(&b, fset, n)
		return b.String()
	}
	for _, p := range ps {
		for _, f := range p.Files {
			for _, d := range f.Decls {
				switch d := d.(type) {
				case *ast.FuncDecl:
					if !d.Name.IsExported() || d.Recv != nil && !ast.IsExported(recvName(d.Recv.List[0].Type)) {
						continue
					}
					callables++
					d.Body, d.Doc = nil, nil
					decls = append(decls, print(d))
				case *ast.GenDecl:
					for _, s := range d.Specs {
						switch s := s.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() {
								types++
								s.Doc, s.Comment = nil, nil
								decls = append(decls, "type "+print(s))
							}
						case *ast.ValueSpec:
							for _, n := range s.Names {
								if n.IsExported() {
									decls = append(decls, d.Tok.String()+" "+n.Name)
								}
							}
						}
					}
				}
			}
		}
	}
	slices.Sort(decls)
	return strings.Join(decls, "\n") + "\n", types, callables, nil
}

func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.IndexExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// license names the license of the module in dir from its LICENSE file.
func license(dir string) string {
	for _, n := range []string{"LICENSE", "LICENSE.md", "LICENSE.txt", "COPYING"} {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			continue
		}
		t := string(b)
		for _, l := range []struct{ id, marker string }{{"Apache-2.0", "Apache License"}, {"MIT", "Permission is hereby granted, free of charge"},
			{"BSD-3-Clause", "Neither the name"}, {"BSD-2-Clause", "Redistribution and use in source and binary forms"}, {"ISC", "Permission to use, copy, modify, and/or distribute"}, {"MPL-2.0", "Mozilla Public License"}} {
			if strings.Contains(t, l.marker) {
				return l.id
			}
		}
		return "unrecognized (" + n + ")"
	}
	return "missing"
}
