package store

import "testing"

func TestPartSizeFor(t *testing.T) {
	cases := []struct {
		size    int64
		minPart int64
		maxCnt  int64
	}{
		{1 << 20, PartBytes, 1},
		{100 << 20, PartBytes, 13},
		{4 << 30, PartBytes, 512},
		{18 << 30, 36 << 20, 512},
		{100 << 30, 200 << 20, 512},
	}
	for _, tc := range cases {
		ps := partSizeFor(tc.size)
		if ps < tc.minPart {
			t.Errorf("partSizeFor(%d) = %d, want >= %d", tc.size, ps, tc.minPart)
		}
		if ps%(1<<20) != 0 {
			t.Errorf("partSizeFor(%d) = %d, not a multiple of 1 MiB", tc.size, ps)
		}
		cnt := (tc.size + ps - 1) / ps
		if cnt > tc.maxCnt {
			t.Errorf("partSizeFor(%d) = %d gives %d parts, want <= %d", tc.size, ps, cnt, tc.maxCnt)
		}
	}
}
