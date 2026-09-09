package shared

import (
	"testing"

	"github.com/lcy03406/actor-go/actor"
)

type cacheBoardId struct{ ID string }

func (cacheBoardId) ActorType() actor.ActorType { return "cache_test_board" }
func (id cacheBoardId) String() string          { return "board-" + id.ID }

type cacheReaderId struct{ ID string }

func (cacheReaderId) ActorType() actor.ActorType { return "cache_test_reader" }
func (id cacheReaderId) String() string          { return "reader-" + id.ID }

type cacheReaderState struct {
	Board Cache[cacheReaderId, cacheBoardId, cacheReaderState, string]
}

func TestCache_InitiallyStale(t *testing.T) {
	var c Cache[cacheReaderId, cacheBoardId, cacheReaderState, string]
	if !c.Stale() {
		t.Fatal("fresh cache should be stale")
	}
}

func TestCache_SetMarksLoaded(t *testing.T) {
	var c Cache[cacheReaderId, cacheBoardId, cacheReaderState, string]
	c.Set(Snapshot[string]{Version: 3, Data: "v3"})
	if c.Stale() {
		t.Fatal("after Set should be loaded")
	}
	if c.Version() != 3 || c.Data() != "v3" {
		t.Fatalf("after Set: version=%d data=%q", c.Version(), c.Data())
	}
}

func TestCache_SetReplacesData(t *testing.T) {
	var c Cache[cacheReaderId, cacheBoardId, cacheReaderState, string]
	c.Set(Snapshot[string]{Version: 1, Data: "v1"})
	c.Set(Snapshot[string]{Version: 2, Data: "v2"})
	if c.Version() != 2 || c.Data() != "v2" {
		t.Fatalf("after second Set: version=%d data=%q", c.Version(), c.Data())
	}
}

func TestVersioned_SetBumpsVersion(t *testing.T) {
	var v Versioned[cacheReaderId, string]
	if v.Version() != 0 || v.Data() != "" {
		t.Fatalf("zero value: version=%d data=%q", v.Version(), v.Data())
	}
	v.Set("a")
	if v.Version() != 1 || v.Data() != "a" {
		t.Fatalf("after Set(a): version=%d data=%q", v.Version(), v.Data())
	}
	v.Set("b")
	if v.Version() != 2 || v.Data() != "b" {
		t.Fatalf("after Set(b): version=%d data=%q", v.Version(), v.Data())
	}
}

func TestVersioned_Update(t *testing.T) {
	var v Versioned[cacheReaderId, int]
	v.Set(1)
	v.Update(func(n *int) { *n += 10 })
	if v.Data() != 11 || v.Version() != 2 {
		t.Fatalf("after Update: data=%d version=%d", v.Data(), v.Version())
	}
}

func TestVersioned_ReaderRegistry(t *testing.T) {
	var v Versioned[cacheReaderId, string]
	r1 := cacheReaderId{ID: "r1"}
	r2 := cacheReaderId{ID: "r2"}

	if got := v.readerIDs(); len(got) != 0 {
		t.Fatalf("zero value: want no readers, got %d", len(got))
	}
	v.addReader(r1)
	v.addReader(r2)
	v.addReader(r1) // 幂等
	if got := v.readerIDs(); len(got) != 2 {
		t.Fatalf("want 2 readers, got %d", len(got))
	}
	v.removeReader(r1)
	if got := v.readerIDs(); len(got) != 1 || got[0] != r2 {
		t.Fatalf("after remove: want [r2], got %v", got)
	}
}
