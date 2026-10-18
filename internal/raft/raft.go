package raft

import (
	"fmt"
	"math/rand"
	"slices"
)

type progressState int

const (
	// probe sends one append at a time until the follower's match is found.
	probe progressState = iota
	// replicate streams appends optimistically after a successful match.
	replicate
	// snapshotting waits for the follower to acknowledge an InstallSnapshot.
	snapshotting
)

// progress is the leader's view of one follower (Raft nextIndex/matchIndex).
type progress struct {
	match, next  uint64
	state        progressState
	paused       bool
	pendingSnap  uint64
	snapTicks    int
	recentActive bool
	// readAck is the highest ReadIndex round the follower confirmed in the
	// current term.
	readAck uint64
}

type pendingRead struct {
	id uint64
}

type Raft struct {
	id    uint64
	peers []uint64

	term  uint64
	vote  uint64
	state StateType
	lead  uint64
	log   *raftLog

	prs   map[uint64]*progress
	votes map[uint64]bool

	electionTick, heartbeatTick int
	randomizedElectionTimeout   int
	electionElapsed             int
	heartbeatElapsed            int
	maxEntriesPerMsg            int
	maxUncommitted              int
	rng                         *rand.Rand

	nextReadID   uint64
	pendingReads []pendingRead

	// snapshot is the latest snapshot, sent to followers that fell behind
	// the compacted log.
	snapshot *Snapshot
	// received is a snapshot from the leader not yet returned by Ready.
	received *Snapshot

	msgs       []Message
	readStates []ReadState
	persisted  HardState
}

// New creates a node from durable state loaded at startup. snap and entries
// may be empty for a fresh node.
func New(cfg Config, hs HardState, snap *Snapshot, entries []Entry) *Raft {
	if cfg.ID == None || !slices.Contains(cfg.Peers, cfg.ID) {
		panic("raft: config must include own ID in Peers")
	}
	if cfg.ElectionTick <= cfg.HeartbeatTick || cfg.HeartbeatTick < 1 {
		panic("raft: ElectionTick must exceed HeartbeatTick >= 1")
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	r := &Raft{
		id:               cfg.ID,
		peers:            slices.Compact(peers),
		term:             hs.Term,
		vote:             hs.Vote,
		log:              newLog(snap, entries),
		electionTick:     cfg.ElectionTick,
		heartbeatTick:    cfg.HeartbeatTick,
		maxEntriesPerMsg: max(cfg.MaxEntriesPerMsg, 1),
		maxUncommitted:   cfg.MaxUncommitted,
		rng:              rand.New(rand.NewSource(cfg.Seed)),
		snapshot:         snap,
		persisted:        hs,
	}
	r.becomeFollower(r.term, None)
	return r
}

func (r *Raft) quorum() int { return len(r.peers)/2 + 1 }

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) resetTimers() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTick + r.rng.Intn(r.electionTick)
}

func (r *Raft) becomeFollower(term, lead uint64) {
	if term != r.term {
		r.term, r.vote = term, None
	}
	r.state, r.lead = Follower, lead
	r.prs, r.votes, r.pendingReads = nil, nil, nil
	r.resetTimers()
}

func (r *Raft) becomePreCandidate() {
	// Pre-vote (Raft thesis 9.6) checks electability without disturbing
	// the cluster's term, so a node returning from a partition cannot
	// depose a healthy leader.
	r.state, r.lead = PreCandidate, None
	r.votes = map[uint64]bool{}
	r.prs, r.pendingReads = nil, nil
	r.resetTimers()
}

func (r *Raft) becomeCandidate() {
	r.term++
	r.vote = r.id
	r.state, r.lead = Candidate, None
	r.votes = map[uint64]bool{}
	r.resetTimers()
}

func (r *Raft) becomeLeader() {
	r.state, r.lead = Leader, r.id
	r.prs = map[uint64]*progress{}
	for _, id := range r.peers {
		r.prs[id] = &progress{next: r.log.lastIndex() + 1, recentActive: true}
	}
	r.resetTimers()
	r.appendEntries(Entry{Type: EntryNoop})
	r.bcastAppend()
}

func (r *Raft) campaign(preVote bool) {
	if preVote {
		r.becomePreCandidate()
	} else {
		r.becomeCandidate()
	}
	msgType, term := MsgVote, r.term
	if preVote {
		msgType, term = MsgPreVote, r.term+1
	}
	if r.poll(r.id, true, preVote) {
		return
	}
	for _, id := range r.peers {
		if id != r.id {
			r.send(Message{Type: msgType, To: id, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
		}
	}
}

// poll records a vote and advances the election if it is decided. It
// returns true once the node has won or lost.
func (r *Raft) poll(id uint64, granted, preVote bool) bool {
	if _, seen := r.votes[id]; !seen {
		r.votes[id] = granted
	}
	yes, no := 0, 0
	for _, g := range r.votes {
		if g {
			yes++
		} else {
			no++
		}
	}
	switch {
	case yes >= r.quorum():
		if preVote {
			r.campaign(false)
		} else {
			r.becomeLeader()
		}
		return true
	case no >= r.quorum():
		r.becomeFollower(r.term, None)
		return true
	}
	return false
}

// Tick advances logical time by one heartbeat interval unit.
func (r *Raft) Tick() {
	r.electionElapsed++
	if r.state != Leader {
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.campaign(true)
		}
		return
	}
	r.heartbeatElapsed++
	for _, id := range r.peers {
		if pr := r.prs[id]; id != r.id && pr.state == snapshotting {
			if pr.snapTicks++; pr.snapTicks >= r.electionTick {
				// The snapshot or its acknowledgement was probably lost.
				pr.state, pr.paused = probe, false
				pr.next = pr.match + 1
				r.sendAppend(id)
			}
		}
	}
	if r.electionElapsed >= r.electionTick {
		r.electionElapsed = 0
		// Check quorum: step down if a majority has been silent for an
		// election timeout, so a partitioned leader stops accepting work.
		active := 0
		for _, id := range r.peers {
			if pr := r.prs[id]; id == r.id || pr.recentActive {
				active++
			}
			r.prs[id].recentActive = false
		}
		if active < r.quorum() {
			r.becomeFollower(r.term, None)
			return
		}
	}
	if r.heartbeatElapsed >= r.heartbeatTick {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat(r.lastReadID())
	}
}

func (r *Raft) lastReadID() uint64 {
	if n := len(r.pendingReads); n > 0 {
		return r.pendingReads[n-1].id
	}
	return 0
}

// inLease reports whether this node heard from a leader within the minimum
// election timeout; such nodes ignore vote requests (Raft thesis 4.2.3).
func (r *Raft) inLease() bool {
	return r.lead != None && r.electionElapsed < r.electionTick
}

// Step processes a message from a peer.
func (r *Raft) Step(m Message) {
	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && r.inLease() {
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// A pre-vote request never changes our term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// Granted pre-votes carry the proposed future term.
		case m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat:
			// Tell a stale leader about the newer term so it steps down;
			// with pre-vote it may otherwise never learn of it.
			r.send(Message{Type: MsgAppResp, To: m.From})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		r.handleVote(m)
		return
	}
	switch r.state {
	case Leader:
		r.stepLeader(m)
	case Candidate, PreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
}

func (r *Raft) handleVote(m Message) {
	canVote := r.vote == m.From ||
		(r.vote == None && r.lead == None) ||
		(m.Type == MsgPreVote && m.Term > r.term)
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}
	if canVote && r.log.isUpToDate(m.Index, m.LogTerm) {
		r.send(Message{Type: respType, To: m.From, Term: m.Term})
		if m.Type == MsgVote {
			r.electionElapsed = 0
			r.vote = m.From
		}
		return
	}
	r.send(Message{Type: respType, To: m.From, Reject: true})
}

func (r *Raft) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		// A leader exists for this term.
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case MsgPreVoteResp:
		if r.state == PreCandidate {
			r.poll(m.From, !m.Reject, true)
		}
	case MsgVoteResp:
		if r.state == Candidate {
			r.poll(m.From, !m.Reject, false)
		}
	}
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed, r.lead = 0, m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed, r.lead = 0, m.From
		r.log.commitTo(min(m.Commit, r.log.lastIndex()))
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
	case MsgSnap:
		r.electionElapsed, r.lead = 0, m.From
		r.handleSnapshot(m)
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.log.committed {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.committed})
		return
	}
	if last, ok := r.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
		return
	}
	// Skip back over entries whose term exceeds the leader's term at the
	// probe index; none of them can match.
	hint := min(m.Index, r.log.lastIndex())
	for hint > r.log.committed {
		if t, _ := r.log.term(hint); t <= m.LogTerm {
			break
		}
		hint--
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, Hint: hint})
}

func (r *Raft) handleSnapshot(m Message) {
	s := m.Snapshot
	switch {
	case s.Index <= r.log.committed:
		// Already have it.
	case r.log.matchTerm(s.Index, s.Term):
		// Our log contains the snapshot's last entry; only commit advances.
		r.log.commitTo(s.Index)
	default:
		r.log.restore(s)
		r.snapshot, r.received = s, s
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.committed})
}

func (r *Raft) stepLeader(m Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.recentActive = true
		if m.Reject {
			r.handleReject(m.From, pr, m)
			return
		}
		if m.Index > pr.match {
			pr.match = m.Index
		}
		if pr.next <= pr.match {
			pr.next = pr.match + 1
		}
		switch {
		case pr.state == snapshotting && pr.match >= pr.pendingSnap:
			pr.state, pr.paused = replicate, false
		case pr.state == probe:
			pr.state, pr.paused = replicate, false
		}
		if r.maybeCommit() {
			r.bcastAppend()
		} else if pr.match < r.log.lastIndex() && pr.state == replicate && pr.next <= r.log.lastIndex() {
			r.sendAppend(m.From)
		}
	case MsgHeartbeatResp:
		pr.recentActive = true
		pr.paused = false
		if m.Context > pr.readAck {
			pr.readAck = m.Context
			r.maybeReleaseReads()
		}
		if pr.match < r.log.lastIndex() && pr.state != snapshotting {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleReject(id uint64, pr *progress, m Message) {
	switch pr.state {
	case replicate:
		if m.Index <= pr.match {
			return // stale
		}
		pr.state = probe
		pr.next = pr.match + 1
	case probe:
		if m.Index != pr.next-1 {
			return // stale
		}
		pr.next = max(min(m.Index, m.Hint+1), pr.match+1, 1)
	default:
		return
	}
	pr.paused = false
	r.sendAppend(id)
}

// maybeCommit advances the commit index to the highest index replicated on
// a quorum, but only for an entry of the current term (Raft section 5.4.2).
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, id := range r.peers {
		if id == r.id {
			matches = append(matches, r.log.lastIndex())
		} else {
			matches = append(matches, r.prs[id].match)
		}
	}
	slices.Sort(matches)
	n := matches[len(matches)-r.quorum()]
	if n > r.log.committed && r.log.matchTerm(n, r.term) {
		r.log.commitTo(n)
		r.maybeReleaseReads()
		return true
	}
	return false
}

func (r *Raft) sendAppend(to uint64) {
	pr := r.prs[to]
	if pr.paused || pr.state == snapshotting {
		return
	}
	prevIndex := pr.next - 1
	prevTerm, ok := r.log.term(prevIndex)
	if !ok {
		// The follower needs entries we compacted: send the snapshot.
		if r.snapshot == nil || r.snapshot.Index < prevIndex {
			panic(fmt.Sprintf("raft: missing term for index %d without snapshot", prevIndex))
		}
		r.send(Message{Type: MsgSnap, To: to, Snapshot: r.snapshot})
		pr.state, pr.pendingSnap, pr.snapTicks = snapshotting, r.snapshot.Index, 0
		return
	}
	last := min(r.log.lastIndex(), prevIndex+uint64(r.maxEntriesPerMsg))
	ents := r.log.slice(pr.next, last)
	r.send(Message{Type: MsgApp, To: to, Index: prevIndex, LogTerm: prevTerm, Entries: ents, Commit: r.log.committed})
	switch pr.state {
	case replicate:
		pr.next = last + 1
	case probe:
		pr.paused = true
	}
}

func (r *Raft) bcastAppend() {
	for _, id := range r.peers {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) bcastHeartbeat(readID uint64) {
	for _, id := range r.peers {
		if id != r.id {
			// Never advance a follower's commit past entries it is known to
			// share with us.
			commit := min(r.prs[id].match, r.log.committed)
			r.send(Message{Type: MsgHeartbeat, To: id, Commit: commit, Context: readID})
		}
	}
}

func (r *Raft) appendEntries(ents ...Entry) {
	last := r.log.lastIndex()
	for i := range ents {
		ents[i].Term, ents[i].Index = r.term, last+1+uint64(i)
	}
	r.log.append(ents...)
	if r.maybeCommit() {
		r.bcastAppend()
	}
}

// Propose appends data to the leader's log, returning its index and term.
// The proposal succeeds only if the entry at that index is later applied
// with the same term.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	if r.maxUncommitted > 0 && r.log.lastIndex()-r.log.committed >= uint64(r.maxUncommitted) {
		return 0, 0, ErrProposalLimit
	}
	r.appendEntries(Entry{Type: EntryNormal, Data: data})
	r.bcastAppend()
	return r.log.lastIndex(), r.term, nil
}

// ReadIndex starts a linearizable read (Raft thesis 6.4). The returned ID
// appears in a later Ready.ReadStates once a quorum has confirmed this node
// is still leader and it has committed an entry in its term.
func (r *Raft) ReadIndex() (uint64, error) {
	if r.state != Leader {
		return 0, ErrNotLeader
	}
	r.nextReadID++
	id := r.nextReadID
	r.pendingReads = append(r.pendingReads, pendingRead{id: id})
	if len(r.peers) == 1 {
		r.maybeReleaseReads()
	} else {
		r.bcastHeartbeat(id)
	}
	return id, nil
}

func (r *Raft) maybeReleaseReads() {
	// Until an entry of this term commits, the leader's commit index may
	// lag what earlier leaders acknowledged.
	if len(r.pendingReads) == 0 || !r.log.matchTerm(r.log.committed, r.term) {
		return
	}
	// A read is confirmed when a quorum acknowledged a heartbeat sent after
	// it was registered. Its read index is the current commit index, which
	// covers every write acknowledged before the read began.
	acks := make([]uint64, 0, len(r.peers))
	for _, id := range r.peers {
		if id == r.id {
			acks = append(acks, r.nextReadID)
		} else {
			acks = append(acks, r.prs[id].readAck)
		}
	}
	slices.Sort(acks)
	confirmed := r.nextReadID
	n := 0
	for _, p := range r.pendingReads {
		if p.id > confirmed {
			break
		}
		r.readStates = append(r.readStates, ReadState{ID: p.id, Index: r.log.committed})
		n++
	}
	r.pendingReads = r.pendingReads[n:]
}

// Compact records a snapshot of the state machine through index, which must
// be applied, and discards the covered log prefix. It returns the snapshot
// and the entries that remain after it.
func (r *Raft) Compact(index uint64, data []byte) (Snapshot, []Entry) {
	if index > r.log.applied {
		panic(fmt.Sprintf("raft: compact %d beyond applied %d", index, r.log.applied))
	}
	t, ok := r.log.term(index)
	if !ok {
		panic(fmt.Sprintf("raft: compact index %d unavailable", index))
	}
	r.log.compact(index)
	r.snapshot = &Snapshot{Index: index, Term: t, Data: data}
	return *r.snapshot, r.log.slice(index+1, r.log.lastIndex())
}

func (r *Raft) HasReady() bool {
	return len(r.msgs) > 0 || len(r.readStates) > 0 || r.received != nil ||
		r.log.unstable <= r.log.lastIndex() || r.log.committed > r.log.applied ||
		r.hardState() != r.persisted
}

func (r *Raft) hardState() HardState { return HardState{Term: r.term, Vote: r.vote} }

// Ready returns pending work. The caller must call Advance before the next
// Step, Tick, Propose, ReadIndex, or Compact.
func (r *Raft) Ready() Ready {
	rd := Ready{
		SoftState:  SoftState{Lead: r.lead, State: r.state},
		HardState:  r.hardState(),
		Snapshot:   r.received,
		Messages:   r.msgs,
		ReadStates: r.readStates,
	}
	rd.HardStateChanged = rd.HardState != r.persisted
	if r.log.unstable <= r.log.lastIndex() {
		rd.Entries = r.log.slice(max(r.log.unstable, r.log.snapIndex+1), r.log.lastIndex())
	}
	if r.log.committed > r.log.applied {
		rd.CommittedEntries = r.log.slice(r.log.applied+1, r.log.committed)
	}
	return rd
}

func (r *Raft) Advance(rd Ready) {
	r.persisted = rd.HardState
	if n := len(rd.Entries); n > 0 && rd.Entries[n-1].Index >= r.log.unstable {
		r.log.unstable = rd.Entries[n-1].Index + 1
	}
	if n := len(rd.CommittedEntries); n > 0 {
		r.log.applied = rd.CommittedEntries[n-1].Index
	}
	if rd.Snapshot != nil && r.received == rd.Snapshot {
		r.received = nil
	}
	r.msgs, r.readStates = nil, nil
}

type Status struct {
	ID        uint64
	Term      uint64
	Vote      uint64
	State     StateType
	Lead      uint64
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	SnapIndex uint64
}

func (r *Raft) Status() Status {
	return Status{ID: r.id, Term: r.term, Vote: r.vote, State: r.state, Lead: r.lead,
		Commit: r.log.committed, Applied: r.log.applied, LastIndex: r.log.lastIndex(),
		SnapIndex: r.log.snapIndex}
}
