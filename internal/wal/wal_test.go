package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"quorumdb/internal/raft"
)

func entries(lo, hi, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := lo; i <= hi; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprint("e", i, "t", term))})
	}
	return out
}

func equal(a, b []raft.Entry) bool {
	return slices.EqualFunc(a, b, func(x, y raft.Entry) bool {
		return x.Index == y.Index && x.Term == y.Term && x.Type == y.Type && string(x.Data) == string(y.Data)
	})
}

func reopen(t *testing.T, w *WAL, dir string) (*WAL, raft.HardState, *raft.Snapshot, []raft.Entry) {
	t.Helper()
	if w != nil {
		w.Close()
	}
	w, hs, snap, ents, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, hs, snap, ents
}

func TestSaveTruncateAndReload(t *testing.T) {
	dir := t.TempDir()
	w, _, _, _ := reopen(t, nil, dir)
	if err := w.Save(raft.HardState{Term: 2, Vote: 3}, nil, entries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	// A new leader overwrites the uncommitted suffix from index 4.
	if err := w.Save(raft.HardState{Term: 3, Vote: 1}, nil, entries(4, 6, 3)); err != nil {
		t.Fatal(err)
	}
	w, hs, snap, ents := reopen(t, w, dir)
	want := append(entries(1, 3, 1), entries(4, 6, 3)...)
	if hs != (raft.HardState{Term: 3, Vote: 1}) || snap != nil || !equal(ents, want) {
		t.Fatalf("reloaded hs=%v snap=%v ents=%v", hs, snap, ents)
	}
	if err := w.Save(hs, nil, entries(8, 8, 3)); err == nil {
		t.Fatal("accepted a gap in the log")
	}
}

func TestTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	w, _, _, _ := reopen(t, nil, dir)
	w.Save(raft.HardState{Term: 1}, nil, entries(1, 3, 1))
	w.Close()
	path := filepath.Join(dir, "wal.log")
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	w, _, _, ents := reopen(t, nil, dir)
	if !equal(ents, entries(1, 2, 1)) {
		t.Fatalf("entries after torn write: %v", ents)
	}
	// Appending after recovery must produce a readable log.
	if err := w.Save(raft.HardState{Term: 1}, nil, entries(3, 4, 1)); err != nil {
		t.Fatal(err)
	}
	_, _, _, ents = reopen(t, w, dir)
	if !equal(ents, entries(1, 4, 1)) {
		t.Fatalf("entries after re-append: %v", ents)
	}
}

func TestCorruptRecordRejected(t *testing.T) {
	dir := t.TempDir()
	w, _, _, _ := reopen(t, nil, dir)
	w.Save(raft.HardState{Term: 1}, nil, entries(1, 3, 1))
	off := w.offsets[1]
	w.Close()
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0xff}, off+12)
	f.Close()
	if w, _, _, _, err := Open(dir); err == nil {
		w.Close()
		t.Fatal("opened a log with a corrupt interior record")
	}
}

func TestCompactAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, _, _, _ := reopen(t, nil, dir)
	w.Save(raft.HardState{Term: 1}, nil, entries(1, 10, 1))
	snap := raft.Snapshot{Index: 6, Term: 1, Data: []byte("image")}
	if err := w.Compact(snap, entries(7, 10, 1)); err != nil {
		t.Fatal(err)
	}
	w.Save(raft.HardState{Term: 1}, nil, entries(11, 11, 1))
	w, _, got, ents := reopen(t, w, dir)
	if got == nil || got.Index != 6 || string(got.Data) != "image" || !equal(ents, entries(7, 11, 1)) {
		t.Fatalf("snapshot %+v entries %v", got, ents)
	}
	// A snapshot from the leader replaces the whole log.
	installed := &raft.Snapshot{Index: 20, Term: 2, Data: []byte("leader")}
	if err := w.Save(raft.HardState{Term: 2}, installed, nil); err != nil {
		t.Fatal(err)
	}
	_, _, got, ents = reopen(t, w, dir)
	if got.Index != 20 || len(ents) != 0 {
		t.Fatalf("after install: snapshot %+v entries %v", got, ents)
	}
}

func TestCrashBetweenSnapshotAndLogRewrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		snap raft.Snapshot
		want []raft.Entry
	}{
		{"local compaction", raft.Snapshot{Index: 4, Term: 1}, entries(5, 6, 1)},
		{"divergent install", raft.Snapshot{Index: 5, Term: 9}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w, _, _, _ := reopen(t, nil, dir)
			w.Save(raft.HardState{Term: 1}, nil, entries(1, 6, 1))
			if err := w.writeSnapshot(tc.snap); err != nil { // crash before rewrite
				t.Fatal(err)
			}
			_, _, snap, ents := reopen(t, w, dir)
			if snap.Index != tc.snap.Index || !equal(ents, tc.want) {
				t.Fatalf("snapshot %d entries %v, want %v", snap.Index, ents, tc.want)
			}
		})
	}
}

func TestExclusiveDirectory(t *testing.T) {
	dir := t.TempDir()
	reopen(t, nil, dir)
	if w, _, _, _, err := Open(dir); err == nil {
		w.Close()
		t.Fatal("two processes opened the same directory")
	}
}
