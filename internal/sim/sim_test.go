// Package sim runs whole clusters in a seeded, single-threaded discrete
// event simulation with an adversarial network and crashes, then checks the
// recorded client history for linearizability.
package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"quorumdb/internal/kv"
	lin "quorumdb/internal/linearizability"
	"quorumdb/internal/node"
	"quorumdb/internal/raft"
)

// Simulated time units; one Raft tick is 100 units.
const (
	tickUnits     = 100
	clientTimeout = 4000
)

type event struct {
	at  int64
	seq uint64
	fn  func()
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	return q[i].at < q[j].at || (q[i].at == q[j].at && q[i].seq < q[j].seq)
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

type options struct {
	nodes, clients int
	chaos          int64 // units of fault injection
	snapshotEvery  uint64
}

type cluster struct {
	t    *testing.T
	opts options
	rng  *rand.Rand
	now  int64
	seq  uint64
	q    eventQueue

	ids         []uint64
	nodes       map[uint64]*node.Node // nil while crashed
	storage     map[uint64]*node.MemoryStorage
	incarnation map[uint64]int
	group       map[uint64]int // nodes communicate only within a group
	paused      map[uint64]bool
	held        map[uint64][]func() // work arriving at a paused node
	dropRate    float64
	longDelay   float64

	leaders  map[uint64]uint64 // term -> leader
	appliedL map[uint64]raft.Entry
	failure  error

	clients []*client
	history []porcupine.Operation
	stopOps bool
}

func newCluster(t *testing.T, seed int64, opts options) *cluster {
	c := &cluster{t: t, opts: opts, rng: rand.New(rand.NewSource(seed)),
		nodes: map[uint64]*node.Node{}, storage: map[uint64]*node.MemoryStorage{},
		incarnation: map[uint64]int{}, group: map[uint64]int{},
		paused: map[uint64]bool{}, held: map[uint64][]func(){},
		leaders: map[uint64]uint64{}, appliedL: map[uint64]raft.Entry{}}
	for i := 1; i <= opts.nodes; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		c.storage[id] = &node.MemoryStorage{}
		c.start(id)
	}
	for i := range opts.clients {
		cl := &client{c: c, id: i, name: fmt.Sprintf("client-%d-%d", seed, i)}
		c.clients = append(c.clients, cl)
		c.after(int64(c.rng.Intn(500)), cl.next)
	}
	return c
}

func (c *cluster) after(d int64, fn func()) {
	c.seq++
	heap.Push(&c.q, event{at: c.now + d, seq: c.seq, fn: fn})
}

func (c *cluster) fail(format string, args ...any) {
	if c.failure == nil {
		c.failure = fmt.Errorf("t=%d: %s", c.now, fmt.Sprintf(format, args...))
	}
}

func (c *cluster) start(id uint64) {
	c.incarnation[id]++
	inc := c.incarnation[id]
	s := c.storage[id]
	cfg := node.Config{
		Raft: raft.Config{ID: id, Peers: c.ids, ElectionTick: 10, HeartbeatTick: 1,
			MaxEntriesPerMsg: 8, MaxUncommitted: 256, Seed: c.rng.Int63()},
		SnapshotEntries: c.opts.snapshotEvery,
		OnApply:         func(e raft.Entry) { c.checkApply(id, e) },
	}
	n, err := node.New(cfg, s, s.HardState, s.Snapshot, slices.Clone(s.Entries),
		func(msgs []raft.Message) { c.transmit(msgs) })
	if err != nil {
		c.fail("restart node %d: %v", id, err)
		return
	}
	c.nodes[id] = n
	// Clocks run at different rates.
	period := int64(tickUnits - 25 + c.rng.Intn(51))
	var tick func()
	tick = func() {
		if c.incarnation[id] != inc || c.nodes[id] == nil {
			return
		}
		if !c.paused[id] {
			n.Tick()
			n.Process()
			c.observe(id)
		}
		c.after(period, tick)
	}
	c.after(int64(c.rng.Intn(tickUnits)), tick)
}

func (c *cluster) crash(id uint64) {
	if n := c.nodes[id]; n != nil {
		c.nodes[id] = nil
		n.Stop()
	}
	// Work held for the dead process is lost.
	delete(c.paused, id)
	delete(c.held, id)
}

// at runs fn on node id now, or when it resumes if paused.
func (c *cluster) at(id uint64, fn func()) {
	if c.paused[id] {
		c.held[id] = append(c.held[id], fn)
		return
	}
	fn()
}

// resume ends a process pause; held work runs in arbitrary order, as it
// would when a stalled process drains several sockets.
func (c *cluster) resume(id uint64) {
	if !c.paused[id] {
		return
	}
	delete(c.paused, id)
	held := c.held[id]
	delete(c.held, id)
	c.rng.Shuffle(len(held), func(i, j int) { held[i], held[j] = held[j], held[i] })
	for _, fn := range held {
		c.after(0, fn)
	}
}

// observe checks election safety: at most one leader per term.
func (c *cluster) observe(id uint64) {
	n := c.nodes[id]
	if n == nil {
		return
	}
	if err := n.Err(); err != nil && !errors.Is(err, node.ErrStopped) {
		c.fail("node %d failed: %v", id, err)
	}
	st := n.Status()
	if st.State != raft.Leader {
		return
	}
	if prev, ok := c.leaders[st.Term]; ok && prev != id {
		c.fail("two leaders in term %d: %d and %d", st.Term, prev, id)
	}
	c.leaders[st.Term] = id
}

// checkApply checks state machine safety: every node applies the same
// entry at each index.
func (c *cluster) checkApply(id uint64, e raft.Entry) {
	prev, ok := c.appliedL[e.Index]
	if !ok {
		c.appliedL[e.Index] = e
		return
	}
	if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
		c.fail("node %d applied %d@%d %q; another applied %d@%d %q", id, e.Index, e.Term, e.Data,
			prev.Index, prev.Term, prev.Data)
	}
}

func (c *cluster) delay() int64 {
	if c.rng.Float64() < c.longDelay {
		return int64(50 + c.rng.Intn(1500)) // reorders past later messages
	}
	return int64(1 + c.rng.Intn(40))
}

func (c *cluster) transmit(msgs []raft.Message) {
	for _, m := range msgs {
		if c.rng.Float64() < c.dropRate || c.group[m.From] != c.group[m.To] {
			continue
		}
		deliver := func() {
			if c.group[m.From] != c.group[m.To] {
				return
			}
			c.at(m.To, func() {
				if n := c.nodes[m.To]; n != nil {
					n.Step(m)
					n.Process()
					c.observe(m.To)
				}
			})
		}
		c.after(c.delay(), deliver)
		if c.rng.Float64() < c.dropRate/2 {
			c.after(c.delay(), deliver) // duplicate
		}
	}
}

// nemesis injects one random fault and schedules the next.
func (c *cluster) nemesis() {
	if c.now >= c.opts.chaos {
		return
	}
	switch k := c.rng.Intn(12); {
	case k < 3: // partition into two sides
		perm := c.rng.Perm(len(c.ids))
		cut := 1 + c.rng.Intn(len(c.ids)-1)
		for i, p := range perm {
			c.group[c.ids[p]] = min(i/cut, 1)
		}
	case k < 4: // isolate the current leader
		for _, id := range c.ids {
			c.group[id] = 0
			if n := c.nodes[id]; n != nil && n.Status().State == raft.Leader {
				c.group[id] = 1
			}
		}
	case k < 6:
		clear(c.group)
	case k < 8: // crash a node and restart it later
		id := c.ids[c.rng.Intn(len(c.ids))]
		if c.nodes[id] != nil {
			c.crash(id)
			c.after(int64(200+c.rng.Intn(3000)), func() {
				if c.nodes[id] == nil {
					c.start(id)
				}
			})
		}
	case k < 10: // pause a process (GC, SIGSTOP), often the leader
		id := c.ids[c.rng.Intn(len(c.ids))]
		for _, cand := range c.ids {
			if n := c.nodes[cand]; n != nil && n.Status().State == raft.Leader && c.rng.Intn(2) == 0 {
				id = cand
			}
		}
		if c.nodes[id] != nil && !c.paused[id] {
			c.paused[id] = true
			c.after(int64(500+c.rng.Intn(4000)), func() { c.resume(id) })
		}
	default:
		c.dropRate = []float64{0, 0.02, 0.1, 0.25}[c.rng.Intn(4)]
		c.longDelay = []float64{0, 0.05, 0.2}[c.rng.Intn(3)]
	}
	c.after(int64(300+c.rng.Intn(2000)), c.nemesis)
}

func (c *cluster) heal() {
	clear(c.group)
	for _, id := range c.ids {
		c.resume(id)
	}
	c.dropRate, c.longDelay = 0, 0
	for _, id := range c.ids {
		if c.nodes[id] == nil {
			c.start(id)
		}
	}
}

func (c *cluster) runUntil(deadline int64, done func() bool) bool {
	for c.q.Len() > 0 && c.failure == nil {
		if done != nil && done() {
			return true
		}
		e := heap.Pop(&c.q).(event)
		if e.at > deadline {
			heap.Push(&c.q, e)
			return done == nil
		}
		c.now = e.at
		e.fn()
	}
	return done == nil || done()
}

// client issues one operation at a time, retrying until it succeeds.
type client struct {
	c       *cluster
	id      int
	name    string
	seq     uint64
	target  uint64
	attempt int
	busy    bool
	done    int

	in   lin.Input
	call int64
}

func (cl *client) next() {
	c := cl.c
	if c.stopOps {
		return
	}
	key := string(rune('a' + c.rng.Intn(3)))
	switch r := c.rng.Intn(10); {
	case r < 5:
		cl.in = lin.Input{Op: "get", Key: key}
	case r < 9:
		cl.seq++
		cl.in = lin.Input{Op: kv.OpPut, Key: key, Value: fmt.Sprintf("%d.%d", cl.id, cl.seq)}
	default:
		cl.seq++
		cl.in = lin.Input{Op: kv.OpDelete, Key: key}
	}
	cl.call, cl.busy = c.now, true
	cl.send()
}

func (cl *client) send() {
	c := cl.c
	cl.attempt++
	attempt := cl.attempt
	if cl.target == 0 || c.rng.Intn(5) == 0 {
		cl.target = c.ids[c.rng.Intn(len(c.ids))]
	}
	target := cl.target
	reply := func(out lin.Output, err error) {
		// The reply crosses the network back to the client.
		if c.rng.Float64() < c.dropRate {
			return
		}
		c.after(c.delay(), func() { cl.receive(attempt, out, err) })
	}
	c.after(c.delay(), func() {
		c.at(target, func() {
			n := c.nodes[target]
			if n == nil {
				return // connection refused; the client times out
			}
			if cl.in.Op == "get" {
				n.Read(cl.in.Key, func(v string, found bool, err error) { reply(lin.Output{Value: v, Found: found}, err) })
			} else {
				cmd := kv.Command{ClientID: cl.name, Seq: cl.seq, Op: cl.in.Op, Key: cl.in.Key, Value: cl.in.Value}
				n.Write(cmd, func(err error) { reply(lin.Output{}, err) })
			}
			n.Process()
			c.observe(target)
		})
	})
	c.after(clientTimeout, func() {
		if cl.attempt == attempt && cl.busy {
			cl.target = 0
			cl.send()
		}
	})
}

func (cl *client) receive(attempt int, out lin.Output, err error) {
	c := cl.c
	if attempt != cl.attempt || !cl.busy {
		return
	}
	if err != nil {
		var nl *node.NotLeaderError
		if errors.As(err, &nl) {
			cl.target = nl.Leader
		} else {
			cl.target = 0
		}
		// Retry with the same sequence number; deduplication keeps the
		// write at most once.
		cl.attempt++
		c.after(int64(20+c.rng.Intn(100)), cl.send)
		return
	}
	c.history = append(c.history, porcupine.Operation{
		ClientId: cl.id, Input: cl.in, Call: cl.call, Output: out, Return: c.now})
	cl.busy = false
	cl.done++
	c.after(int64(c.rng.Intn(200)), cl.next)
}

func runSeed(t *testing.T, seed int64, opts options) {
	c := newCluster(t, seed, opts)
	c.after(1000, c.nemesis)
	c.runUntil(opts.chaos, nil)

	// Recovery: with faults removed, every client must finish its current
	// operation and complete new ones.
	c.heal()
	healed := c.now
	marks := make([]int, len(c.clients))
	for i, cl := range c.clients {
		marks[i] = cl.done
	}
	progressed := func() bool {
		for i, cl := range c.clients {
			if cl.done < marks[i]+2 {
				return false
			}
		}
		return true
	}
	if !c.runUntil(healed+30000, progressed) && c.failure == nil {
		c.fail("cluster did not recover within 30000 units of healing")
	}
	c.stopOps = true
	idle := func() bool {
		for _, cl := range c.clients {
			if cl.busy {
				return false
			}
		}
		return true
	}
	c.runUntil(c.now+30000, idle)
	// Let replication settle so replicas can be compared.
	converged := func() bool {
		var first []byte
		var applied uint64
		for i, id := range c.ids {
			n := c.nodes[id]
			st := n.Status()
			img := n.StateImage()
			if i == 0 {
				first, applied = img, st.Applied
			} else if st.Applied != applied || string(img) != string(first) {
				return false
			}
		}
		return true
	}
	if !c.runUntil(c.now+20000, converged) && c.failure == nil {
		c.fail("replicas did not converge")
	}
	if c.failure != nil {
		t.Fatalf("seed %d: %v", seed, c.failure)
	}

	result, info := porcupine.CheckOperationsVerbose(lin.Model, c.history, 30*time.Second)
	if result != porcupine.Ok {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("seed-%d.html", seed))
		if dir := os.Getenv("QUORUMDB_VIS_DIR"); dir != "" {
			path = filepath.Join(dir, fmt.Sprintf("seed-%d.html", seed))
		}
		porcupine.VisualizePath(lin.Model, info, path)
		t.Fatalf("seed %d: history of %d operations is %s (visualization: %s)", seed, len(c.history), result, path)
	}
	if len(c.history) < 50 {
		t.Fatalf("seed %d: only %d operations completed", seed, len(c.history))
	}
	t.Logf("seed %d: %d ops linearizable, %d terms, %d entries applied", seed, len(c.history),
		len(c.leaders), len(c.appliedL))
}

func seeds(t *testing.T, def int) []int64 {
	n := def
	if s := os.Getenv("QUORUMDB_SEEDS"); s != "" {
		var err error
		if n, err = strconv.Atoi(s); err != nil {
			t.Fatal(err)
		}
	}
	if testing.Short() {
		n = min(n, 5)
	}
	start := int64(1)
	if s := os.Getenv("QUORUMDB_SEED_START"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		start = v
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = start + int64(i)
	}
	return out
}

func TestLinearizableUnderFaults(t *testing.T) {
	for _, size := range []int{3, 5} {
		for _, seed := range seeds(t, 25) {
			t.Run(fmt.Sprintf("nodes=%d/seed=%d", size, seed), func(t *testing.T) {
				t.Parallel()
				runSeed(t, seed, options{nodes: size, clients: 5, chaos: 150000, snapshotEvery: 16})
			})
		}
	}
}

func TestSimulationIsDeterministic(t *testing.T) {
	run := func() []porcupine.Operation {
		c := newCluster(t, 7, options{nodes: 3, clients: 3, chaos: 10000, snapshotEvery: 8})
		c.after(1000, c.nemesis)
		c.runUntil(10000, nil)
		return c.history
	}
	a, b := run(), run()
	if len(a) == 0 || !slices.Equal(a, b) {
		t.Fatalf("two runs with one seed differ: %d vs %d operations", len(a), len(b))
	}
}
