// Package raft implements the Raft consensus algorithm as a deterministic
// state machine. It performs no I/O: callers feed it ticks and messages, then
// persist, send, and apply what Ready returns before calling Advance.
package raft

import "errors"

// None is the zero node ID, meaning "no node".
const None uint64 = 0

type StateType int

const (
	Follower StateType = iota
	PreCandidate
	Candidate
	Leader
)

func (s StateType) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

type EntryType int

const (
	EntryNormal EntryType = iota
	// EntryNoop is appended by each new leader so it can commit entries
	// from earlier terms (Raft section 5.4.2).
	EntryNoop
)

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

type MessageType int

const (
	MsgPreVote MessageType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
)

func (t MessageType) String() string {
	return [...]string{"PreVote", "PreVoteResp", "Vote", "VoteResp", "App", "AppResp",
		"Heartbeat", "HeartbeatResp", "Snap"}[t]
}

// Message is a Raft RPC. Field meaning depends on Type:
//   - Vote/PreVote: Index and LogTerm describe the candidate's last entry.
//   - App: Index and LogTerm describe the entry preceding Entries.
//   - AppResp: Index is the follower's matching index, or the rejected
//     Index with Hint as the next index the leader should probe from.
//   - Heartbeat/HeartbeatResp: Context carries a ReadIndex round ID.
type Message struct {
	Type     MessageType
	From     uint64
	To       uint64
	Term     uint64
	LogTerm  uint64
	Index    uint64
	Entries  []Entry
	Commit   uint64
	Reject   bool
	Hint     uint64
	Context  uint64
	Snapshot *Snapshot
}

// Snapshot is the state machine image covering all entries through Index.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// HardState must be durable before any message produced with it is sent.
type HardState struct {
	Term uint64
	Vote uint64
}

// ReadState reports that a linearizable read registered with ID may be
// served once the state machine has applied Index.
type ReadState struct {
	ID    uint64
	Index uint64
}

type SoftState struct {
	Lead  uint64
	State StateType
}

// Ready lists work for the caller, in order: persist Snapshot, Entries and
// HardState durably; send Messages; apply Snapshot then CommittedEntries;
// serve ReadStates once applied; then call Advance.
type Ready struct {
	SoftState SoftState
	HardState HardState
	// HardStateChanged reports whether HardState differs from the last
	// persisted value.
	HardStateChanged bool
	// Snapshot, if set, was received from the leader and replaces the
	// whole log.
	Snapshot *Snapshot
	// Entries must be written after discarding any persisted entries at or
	// beyond Entries[0].Index.
	Entries          []Entry
	CommittedEntries []Entry
	Messages         []Message
	ReadStates       []ReadState
}

var (
	ErrNotLeader     = errors.New("raft: not the leader")
	ErrProposalLimit = errors.New("raft: too many uncommitted entries")
)

type Config struct {
	ID    uint64
	Peers []uint64 // all voters, including ID
	// ElectionTick is the base election timeout in ticks; each node waits
	// a random duration in [ElectionTick, 2*ElectionTick) before campaigning.
	ElectionTick  int
	HeartbeatTick int
	// MaxEntriesPerMsg bounds each append message.
	MaxEntriesPerMsg int
	// MaxUncommitted bounds the leader's uncommitted log suffix.
	MaxUncommitted int
	// Seed drives the randomized election timeout.
	Seed int64
}
