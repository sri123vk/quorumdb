package kv

import "testing"

func TestApplyDedupAndSnapshot(t *testing.T) {
	s := New()
	put := Command{ClientID: "c1", Seq: 1, Op: OpPut, Key: "k", Value: "v1"}
	if dup, err := s.Apply(Encode(put)); dup || err != nil {
		t.Fatalf("first apply: dup=%v err=%v", dup, err)
	}
	newer := Command{ClientID: "c1", Seq: 2, Op: OpPut, Key: "k", Value: "v2"}
	s.Apply(Encode(newer))
	// A delayed retry of seq 1 must not roll the value back.
	if dup, _ := s.Apply(Encode(put)); !dup {
		t.Fatal("retry was not detected as duplicate")
	}
	if v, _ := s.Get("k"); v != "v2" {
		t.Fatalf("value = %q", v)
	}
	restored := New()
	if err := restored.Restore(s.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if v, _ := restored.Get("k"); v != "v2" {
		t.Fatalf("restored value = %q", v)
	}
	if dup, _ := restored.Apply(Encode(newer)); !dup {
		t.Fatal("session table not restored")
	}
	s.Apply(Encode(Command{Op: OpDelete, Key: "k"}))
	if _, ok := s.Get("k"); ok {
		t.Fatal("delete without session failed")
	}
}

func TestInvalidCommands(t *testing.T) {
	s := New()
	for _, data := range []string{`nope`, `{"op":"put"}`, `{"op":"x","k":"a"}`, `{"op":"put","k":"a","c":"id"}`} {
		if _, err := s.Apply([]byte(data)); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}
