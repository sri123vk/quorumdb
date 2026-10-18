// Package node connects the Raft core to durable storage and the KV state
// machine. A Node is single-threaded: the caller serializes every method
// call, which keeps the simulation tests deterministic. Tick, Step, Write
// and Read only queue work; Process persists, sends and applies it, so a
// caller may batch several events into one fsync. Callbacks run on the
// caller's goroutine and must not block.
package node

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"quorumdb/internal/kv"
	"quorumdb/internal/raft"
)

// Storage persists Raft state; both methods must be durable on return.
type Storage interface {
	Save(hs raft.HardState, snap *raft.Snapshot, ents []raft.Entry) error
	Compact(snap raft.Snapshot, rest []raft.Entry) error
}

type Config struct {
	Raft raft.Config
	// SnapshotEntries triggers compaction once this many entries have been
	// applied since the last snapshot. Zero disables compaction.
	SnapshotEntries uint64
	// OnApply, if set, observes each committed entry as it is applied.
	OnApply func(raft.Entry)
}

// NotLeaderError reports the node's best guess of the leader, or zero.
type NotLeaderError struct{ Leader uint64 }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not the leader (leader hint %d)", e.Leader)
}

var (
	// ErrUnknownOutcome means a write may or may not have committed. Retrying
	// with the same client ID and sequence is safe.
	ErrUnknownOutcome = errors.New("leadership changed before commit; outcome unknown")
	ErrBusy           = errors.New("too many uncommitted writes")
	ErrStopped        = errors.New("node stopped")
)

type (
	WriteFunc func(err error)
	ReadFunc  func(value string, found bool, err error)
)

type write struct {
	term uint64
	done WriteFunc
}

type read struct {
	key   string
	index uint64 // valid once confirmed
	done  ReadFunc
}

type Node struct {
	r        *raft.Raft
	storage  Storage
	sm       *kv.Store
	send     func([]raft.Message)
	snapshot uint64 // entries per snapshot
	onApply  func(raft.Entry)

	writes    map[uint64]write // by log index
	pending   map[uint64]read  // reads awaiting leadership confirmation
	confirmed []read           // reads awaiting apply, in index order
	applied   uint64
	snapIndex uint64
	soft      raft.SoftState
	err       error
}

// New builds a node from state previously loaded from storage.
func New(cfg Config, storage Storage, hs raft.HardState, snap *raft.Snapshot, ents []raft.Entry,
	send func([]raft.Message)) (*Node, error) {
	n := &Node{
		r:        raft.New(cfg.Raft, hs, snap, ents),
		storage:  storage,
		sm:       kv.New(),
		send:     send,
		snapshot: cfg.SnapshotEntries,
		onApply:  cfg.OnApply,
		writes:   map[uint64]write{},
		pending:  map[uint64]read{},
	}
	if snap != nil {
		if err := n.sm.Restore(snap.Data); err != nil {
			return nil, err
		}
		n.applied, n.snapIndex = snap.Index, snap.Index
	}
	return n, nil
}

func (n *Node) Tick() {
	if n.err == nil {
		n.r.Tick()
	}
}

func (n *Node) Step(m raft.Message) {
	if n.err == nil {
		n.r.Step(m)
	}
}

// Write proposes cmd and calls done once it is applied or has failed.
func (n *Node) Write(cmd kv.Command, done WriteFunc) {
	if n.err != nil {
		done(n.err)
		return
	}
	if err := cmd.Validate(); err != nil {
		done(err)
		return
	}
	index, term, err := n.r.Propose(kv.Encode(cmd))
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		done(&NotLeaderError{Leader: n.r.Status().Lead})
		return
	case errors.Is(err, raft.ErrProposalLimit):
		done(ErrBusy)
		return
	}
	n.writes[index] = write{term: term, done: done}
}

// Read performs a linearizable read of key through the ReadIndex protocol.
func (n *Node) Read(key string, done ReadFunc) {
	if n.err != nil {
		done("", false, n.err)
		return
	}
	id, err := n.r.ReadIndex()
	if err != nil {
		done("", false, &NotLeaderError{Leader: n.r.Status().Lead})
		return
	}
	n.pending[id] = read{key: key, done: done}
}

func (n *Node) Status() raft.Status { return n.r.Status() }

// StateImage returns the encoded state machine, for comparing replicas.
func (n *Node) StateImage() []byte { return n.sm.Snapshot() }

// Len returns the number of keys stored.
func (n *Node) Len() int { return n.sm.Len() }

// Err reports a storage failure that halted the node.
func (n *Node) Err() error { return n.err }

// Stop fails all outstanding requests.
func (n *Node) Stop() {
	if n.err == nil {
		n.err = ErrStopped
	}
	n.failAll(n.err)
}

// Process makes queued work durable, sends messages, applies committed
// entries and completes requests.
func (n *Node) Process() {
	for n.err == nil && n.r.HasReady() {
		rd := n.r.Ready()
		if err := n.storage.Save(rd.HardState, rd.Snapshot, rd.Entries); err != nil {
			// The outcome of a failed write is uncertain; stop rather than
			// risk acknowledging state that is not durable.
			n.err = fmt.Errorf("storage failed; restart required: %w", err)
			n.failAll(n.err)
			return
		}
		if len(rd.Messages) > 0 {
			n.send(rd.Messages)
		}
		if rd.SoftState != n.soft {
			if n.soft.State == raft.Leader && rd.SoftState.State != raft.Leader {
				n.loseLeadership(rd.SoftState.Lead)
			}
			n.soft = rd.SoftState
		}
		for _, rs := range rd.ReadStates {
			if rq, ok := n.pending[rs.ID]; ok {
				delete(n.pending, rs.ID)
				rq.index = rs.Index
				n.confirmed = append(n.confirmed, rq)
			}
		}
		if s := rd.Snapshot; s != nil {
			if err := n.sm.Restore(s.Data); err != nil {
				n.err = err
				n.failAll(err)
				return
			}
			n.applied, n.snapIndex = s.Index, s.Index
			n.failWrites(func(index uint64) bool { return index <= s.Index }, ErrUnknownOutcome)
		}
		for _, e := range rd.CommittedEntries {
			n.apply(e)
		}
		n.serveReads()
		n.r.Advance(rd)
		if err := n.maybeCompact(); err != nil {
			n.err = fmt.Errorf("snapshot failed; restart required: %w", err)
			n.failAll(n.err)
		}
	}
}

func (n *Node) apply(e raft.Entry) {
	var err error
	if e.Type == raft.EntryNormal {
		_, err = n.sm.Apply(e.Data)
	}
	n.applied = e.Index
	if n.onApply != nil {
		n.onApply(e)
	}
	if w, ok := n.writes[e.Index]; ok {
		delete(n.writes, e.Index)
		switch {
		case w.term != e.Term:
			// Another leader's entry replaced ours, so it never committed.
			w.done(ErrUnknownOutcome)
		default:
			w.done(err)
		}
	}
}

func (n *Node) serveReads() {
	i := 0
	for ; i < len(n.confirmed) && n.confirmed[i].index <= n.applied; i++ {
		rq := n.confirmed[i]
		v, ok := n.sm.Get(rq.key)
		rq.done(v, ok, nil)
	}
	n.confirmed = n.confirmed[i:]
}

func (n *Node) loseLeadership(lead uint64) {
	// Unconfirmed reads cannot be served; confirmed ones remain valid.
	for _, id := range slices.Sorted(maps.Keys(n.pending)) {
		n.pending[id].done("", false, &NotLeaderError{Leader: lead})
	}
	clear(n.pending)
	n.failWrites(func(uint64) bool { return true }, ErrUnknownOutcome)
}

func (n *Node) failWrites(match func(uint64) bool, err error) {
	for _, index := range slices.Sorted(maps.Keys(n.writes)) {
		if match(index) {
			w := n.writes[index]
			delete(n.writes, index)
			w.done(err)
		}
	}
}

func (n *Node) failAll(err error) {
	n.failWrites(func(uint64) bool { return true }, err)
	for _, id := range slices.Sorted(maps.Keys(n.pending)) {
		n.pending[id].done("", false, err)
	}
	clear(n.pending)
	for _, rq := range n.confirmed {
		rq.done("", false, err)
	}
	n.confirmed = nil
}

func (n *Node) maybeCompact() error {
	if n.snapshot == 0 || n.applied-n.snapIndex < n.snapshot {
		return nil
	}
	snap, rest := n.r.Compact(n.applied, n.sm.Snapshot())
	n.snapIndex = snap.Index
	return n.storage.Compact(snap, rest)
}

// MemoryStorage keeps durable state in memory. It survives a simulated
// crash because the simulation keeps the object and discards the Node.
type MemoryStorage struct {
	HardState raft.HardState
	Snapshot  *raft.Snapshot
	Entries   []raft.Entry
}

func (m *MemoryStorage) Save(hs raft.HardState, snap *raft.Snapshot, ents []raft.Entry) error {
	m.HardState = hs
	if snap != nil {
		m.Snapshot, m.Entries = snap, nil
	}
	if len(ents) == 0 {
		return nil
	}
	base := uint64(0)
	if m.Snapshot != nil {
		base = m.Snapshot.Index
	}
	first := ents[0].Index
	if first <= base || first > base+uint64(len(m.Entries))+1 {
		return fmt.Errorf("entry %d does not follow log", first)
	}
	m.Entries = append(m.Entries[:first-base-1:first-base-1], ents...)
	return nil
}

func (m *MemoryStorage) Compact(snap raft.Snapshot, rest []raft.Entry) error {
	m.Snapshot, m.Entries = &snap, slices.Clone(rest)
	return nil
}
