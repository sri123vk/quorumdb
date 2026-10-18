package raft

import "fmt"

// raftLog holds the entries after the latest snapshot, in memory. Persistence
// is the caller's job; unstable marks the first index not yet handed out in
// Ready.Entries.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry // entries[i].Index == snapIndex+1+i
	committed uint64
	applied   uint64
	unstable  uint64
}

func newLog(snap *Snapshot, entries []Entry) *raftLog {
	l := &raftLog{}
	if snap != nil {
		l.snapIndex, l.snapTerm = snap.Index, snap.Term
	}
	for i, e := range entries {
		if e.Index != l.snapIndex+1+uint64(i) {
			panic(fmt.Sprintf("raft: non-contiguous log at index %d after snapshot %d", e.Index, l.snapIndex))
		}
	}
	l.entries = append([]Entry(nil), entries...)
	l.committed, l.applied = l.snapIndex, l.snapIndex
	l.unstable = l.lastIndex() + 1
	return l
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of entry i; ok is false if i was compacted or is
// beyond the log.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, ok := l.term(i)
	return ok && t == term
}

// slice returns entries [lo, hi]; lo must be beyond the snapshot.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo > hi {
		return nil
	}
	if lo <= l.snapIndex || hi > l.lastIndex() {
		panic(fmt.Sprintf("raft: slice [%d,%d] out of range (%d,%d]", lo, hi, l.snapIndex, l.lastIndex()))
	}
	return append([]Entry(nil), l.entries[lo-l.snapIndex-1:hi-l.snapIndex]...)
}

// isUpToDate implements the election restriction of Raft section 5.4.1.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	first := ents[0].Index
	if first <= l.committed {
		panic(fmt.Sprintf("raft: overwriting committed entry %d (committed %d)", first, l.committed))
	}
	if first > l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: gap appending %d after %d", first, l.lastIndex()))
	}
	l.entries = append(l.entries[:first-l.snapIndex-1], ents...)
	l.unstable = min(l.unstable, first)
}

// maybeAppend applies an AppendEntries request whose preceding entry is
// (prevIndex, prevTerm). It returns the last index known to match the
// leader, or false if the consistency check failed.
func (l *raftLog) maybeAppend(prevIndex, prevTerm, commit uint64, ents []Entry) (uint64, bool) {
	if !l.matchTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew := prevIndex + uint64(len(ents))
	for i, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			l.append(ents[i:]...)
			break
		}
	}
	l.commitTo(min(commit, lastNew))
	return lastNew, true
}

func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		if i > l.lastIndex() {
			panic(fmt.Sprintf("raft: commit %d beyond last index %d", i, l.lastIndex()))
		}
		l.committed = i
	}
}

// compact discards entries through i, which must already be applied.
func (l *raftLog) compact(i uint64) {
	if i <= l.snapIndex {
		return
	}
	if i > l.applied {
		panic(fmt.Sprintf("raft: compacting unapplied index %d (applied %d)", i, l.applied))
	}
	t, _ := l.term(i)
	l.entries = append([]Entry(nil), l.entries[i-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = i, t
}

// restore replaces the log with an empty one starting after snap.
func (l *raftLog) restore(snap *Snapshot) {
	l.snapIndex, l.snapTerm = snap.Index, snap.Term
	l.entries = nil
	l.committed, l.applied = snap.Index, snap.Index
	l.unstable = snap.Index + 1
}
