package raft

import (
	"fmt"
	"slices"
	"testing"
)

// network delivers messages synchronously among in-memory nodes.
type network struct {
	ids     []uint64
	nodes   map[uint64]*Raft
	cut     map[uint64]bool // isolated nodes
	applied map[uint64][]Entry
	reads   map[uint64][]ReadState
	queue   []Message
}

func testConfig(id uint64, peers []uint64) Config {
	return Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, MaxEntriesPerMsg: 4, Seed: int64(id)}
}

func newNetwork(n int) *network {
	net := &network{nodes: map[uint64]*Raft{}, cut: map[uint64]bool{},
		applied: map[uint64][]Entry{}, reads: map[uint64][]ReadState{}}
	for i := 1; i <= n; i++ {
		net.ids = append(net.ids, uint64(i))
	}
	for _, id := range net.ids {
		net.nodes[id] = New(testConfig(id, net.ids), HardState{}, nil, nil)
	}
	return net
}

func (n *network) collect() {
	for _, id := range n.ids {
		r := n.nodes[id]
		if !r.HasReady() {
			continue
		}
		rd := r.Ready()
		if rd.Snapshot != nil {
			n.applied[id] = nil
		}
		n.applied[id] = append(n.applied[id], rd.CommittedEntries...)
		n.reads[id] = append(n.reads[id], rd.ReadStates...)
		n.queue = append(n.queue, rd.Messages...)
		r.Advance(rd)
	}
}

// drain delivers messages until the network is quiet.
func (n *network) drain() {
	for range 10000 {
		n.collect()
		if len(n.queue) == 0 {
			return
		}
		q := n.queue
		n.queue = nil
		for _, m := range q {
			if !n.cut[m.From] && !n.cut[m.To] {
				n.nodes[m.To].Step(m)
			}
		}
	}
	panic("network did not quiesce")
}

func (n *network) tick(times int) {
	for range times {
		for _, id := range n.ids {
			n.nodes[id].Tick()
		}
		n.drain()
	}
}

func (n *network) leader() uint64 {
	var lead uint64
	for _, id := range n.ids {
		if n.nodes[id].state == Leader && !n.cut[id] {
			if lead != None {
				panic("two connected leaders")
			}
			lead = id
		}
	}
	return lead
}

func (n *network) elect(t *testing.T) uint64 {
	t.Helper()
	for range 100 {
		n.tick(1)
		if lead := n.leader(); lead != None {
			return lead
		}
	}
	t.Fatal("no leader elected")
	return None
}

func (n *network) propose(t *testing.T, id uint64, data string) {
	t.Helper()
	if _, _, err := n.nodes[id].Propose([]byte(data)); err != nil {
		t.Fatal(err)
	}
	n.drain()
}

func normalData(ents []Entry) []string {
	var out []string
	for _, e := range ents {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func TestSingleNode(t *testing.T) {
	n := newNetwork(1)
	lead := n.elect(t)
	n.propose(t, lead, "a")
	if got := normalData(n.applied[lead]); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("applied %v", got)
	}
}

func TestElectionAndReplication(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			n := newNetwork(size)
			lead := n.elect(t)
			term := n.nodes[lead].term
			for _, id := range n.ids {
				if r := n.nodes[id]; r.term != term || r.lead != lead {
					t.Fatalf("node %d term=%d lead=%d, want %d/%d", id, r.term, r.lead, term, lead)
				}
			}
			for i := range 10 {
				n.propose(t, lead, fmt.Sprint(i))
			}
			n.tick(1) // propagate the final commit index
			want := normalData(n.applied[lead])
			if len(want) != 10 {
				t.Fatalf("leader applied %v", want)
			}
			for _, id := range n.ids {
				if got := normalData(n.applied[id]); !slices.Equal(got, want) {
					t.Fatalf("node %d applied %v, want %v", id, got, want)
				}
			}
		})
	}
}

func TestLeaderFailover(t *testing.T) {
	n := newNetwork(3)
	old := n.elect(t)
	n.propose(t, old, "before")
	n.cut[old] = true
	lead := n.elect(t)
	if lead == old {
		t.Fatal("isolated node remained the connected leader")
	}
	n.propose(t, lead, "after")
	// The old leader steps down by check-quorum, then rejoins.
	n.tick(30)
	if n.nodes[old].state == Leader {
		t.Fatal("isolated leader did not step down")
	}
	delete(n.cut, old)
	n.tick(5)
	n.tick(1)
	for _, id := range n.ids {
		if got := normalData(n.applied[id]); !slices.Equal(got, []string{"before", "after"}) {
			t.Fatalf("node %d applied %v", id, got)
		}
	}
}

func TestPreVotePreventsDisruption(t *testing.T) {
	n := newNetwork(3)
	lead := n.elect(t)
	term := n.nodes[lead].term
	var other uint64 = 1
	if other == lead {
		other = 2
	}
	n.cut[other] = true
	n.tick(100)
	if got := n.nodes[other].term; got != term {
		t.Fatalf("partitioned node advanced term %d -> %d", term, got)
	}
	delete(n.cut, other)
	n.tick(5)
	if n.leader() != lead || n.nodes[lead].term != term {
		t.Fatalf("leader disrupted: leader=%d term=%d", n.leader(), n.nodes[lead].term)
	}
}

func TestDivergentLogsConverge(t *testing.T) {
	// Raft Figure 7 style: followers missing entries and holding extra
	// uncommitted entries from older terms.
	ids := []uint64{1, 2, 3, 4, 5}
	ent := func(index, term uint64) Entry {
		return Entry{Index: index, Term: term, Data: []byte(fmt.Sprint(index, "@", term))}
	}
	logs := map[uint64][]Entry{
		1: {ent(1, 1), ent(2, 1), ent(3, 4), ent(4, 4), ent(5, 5)},
		2: {ent(1, 1), ent(2, 1)},
		3: {ent(1, 1), ent(2, 1), ent(3, 2), ent(4, 2), ent(5, 2), ent(6, 2)},
		4: {ent(1, 1), ent(2, 1), ent(3, 4), ent(4, 4)},
		5: {ent(1, 1), ent(2, 1), ent(3, 3), ent(4, 3), ent(5, 3), ent(6, 3), ent(7, 3)},
	}
	n := &network{ids: ids, nodes: map[uint64]*Raft{}, cut: map[uint64]bool{},
		applied: map[uint64][]Entry{}, reads: map[uint64][]ReadState{}}
	for _, id := range ids {
		n.nodes[id] = New(testConfig(id, ids), HardState{Term: 5}, nil, logs[id])
	}
	// Node 1 holds the most up-to-date log; make it campaign first.
	n.nodes[1].randomizedElectionTimeout = 1
	if lead := n.elect(t); lead != 1 {
		t.Fatalf("leader = %d, want 1", lead)
	}
	n.propose(t, 1, "new")
	n.tick(1)
	want := n.nodes[1].log.entries
	for _, id := range ids {
		if got := n.nodes[id].log.entries; !slices.EqualFunc(got, want, entryEqual) {
			t.Fatalf("node %d log %v, want %v", id, got, want)
		}
	}
}

func entryEqual(a, b Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type && string(a.Data) == string(b.Data)
}

func TestNoCommitOfPriorTermByCounting(t *testing.T) {
	// Raft Figure 8: a leader must not commit an entry from an earlier
	// term just because it is stored on a majority.
	ids := []uint64{1, 2, 3, 4, 5}
	r := New(testConfig(1, ids), HardState{Term: 3}, nil,
		[]Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
	r.becomeCandidate()
	r.becomeLeader() // appends a no-op at index 3, term 4
	r.prs[2].match, r.prs[3].match = 2, 2
	if r.maybeCommit() || r.log.committed != 0 {
		t.Fatalf("committed %d by counting replicas of an old-term entry", r.log.committed)
	}
	r.prs[2].match, r.prs[3].match = 3, 3
	if !r.maybeCommit() || r.log.committed != 3 {
		t.Fatalf("committed = %d, want 3", r.log.committed)
	}
}

func TestVoteRestriction(t *testing.T) {
	ids := []uint64{1, 2, 3}
	r := New(testConfig(1, ids), HardState{Term: 2}, nil, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
	for _, tc := range []struct {
		lastIndex, lastTerm uint64
		granted             bool
	}{{1, 2, false}, {3, 1, false}, {2, 2, true}} {
		r.Step(Message{Type: MsgPreVote, From: 2, Term: 3, Index: tc.lastIndex, LogTerm: tc.lastTerm})
		rd := r.Ready()
		r.Advance(rd)
		if len(rd.Messages) != 1 || rd.Messages[0].Reject == tc.granted {
			t.Fatalf("pre-vote for (%d,%d): %+v", tc.lastIndex, tc.lastTerm, rd.Messages)
		}
	}
	if r.term != 2 {
		t.Fatalf("pre-vote changed term to %d", r.term)
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	n := newNetwork(3)
	lead := n.elect(t)
	lagging := uint64(1)
	if lagging == lead {
		lagging = 2
	}
	n.cut[lagging] = true
	for i := range 20 {
		n.propose(t, lead, fmt.Sprint(i))
	}
	r := n.nodes[lead]
	snap, rest := r.Compact(r.log.applied, []byte("image"))
	if len(rest) != 0 || r.log.snapIndex != snap.Index {
		t.Fatalf("compaction left %d entries, snapIndex %d", len(rest), r.log.snapIndex)
	}
	delete(n.cut, lagging)
	n.tick(3)
	f := n.nodes[lagging]
	if f.log.snapIndex != snap.Index || f.log.committed < snap.Index || string(f.snapshot.Data) != "image" {
		t.Fatalf("follower snapIndex=%d committed=%d", f.log.snapIndex, f.log.committed)
	}
	n.propose(t, lead, "after")
	n.tick(1)
	if got := normalData(n.applied[lagging]); !slices.Equal(got, []string{"after"}) {
		t.Fatalf("follower applied after snapshot: %v", got)
	}
}

func TestReadIndexNeedsQuorum(t *testing.T) {
	n := newNetwork(3)
	lead := n.elect(t)
	n.propose(t, lead, "x")
	id, err := n.nodes[lead].ReadIndex()
	if err != nil {
		t.Fatal(err)
	}
	n.drain()
	reads := n.reads[lead]
	if len(reads) != 1 || reads[0].ID != id || reads[0].Index != n.nodes[lead].log.committed {
		t.Fatalf("reads = %+v", reads)
	}
	for _, other := range n.ids {
		if other != lead {
			n.cut[other] = true
		}
	}
	if _, err := n.nodes[lead].ReadIndex(); err != nil {
		t.Fatal(err)
	}
	n.drain()
	if len(n.reads[lead]) != 1 {
		t.Fatal("isolated leader confirmed a read")
	}
	if _, err := n.nodes[lead%3+1].ReadIndex(); err != ErrNotLeader {
		t.Fatalf("follower ReadIndex error = %v", err)
	}
}

func TestStaleLeaderLearnsNewTerm(t *testing.T) {
	ids := []uint64{1, 2, 3}
	r := New(testConfig(1, ids), HardState{Term: 5}, nil, nil)
	r.Step(Message{Type: MsgApp, From: 2, Term: 3})
	rd := r.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgAppResp || rd.Messages[0].Term != 5 {
		t.Fatalf("response %+v", rd.Messages)
	}
}
