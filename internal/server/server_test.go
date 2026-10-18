package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"quorumdb/internal/client"
	lin "quorumdb/internal/linearizability"
)

type testCluster struct {
	t       *testing.T
	cfg     map[uint64]Peer
	dirs    map[uint64]string
	servers map[uint64]*Server
	snap    uint64
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	var l net.Listener
	var err error
	for range 50 { // a just-closed port can take a moment to free
		if l, err = net.Listen("tcp", addr); err == nil {
			return l
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

func newTestCluster(t *testing.T, n int, snapshotEntries uint64) *testCluster {
	c := &testCluster{t: t, cfg: map[uint64]Peer{}, dirs: map[uint64]string{}, servers: map[uint64]*Server{},
		snap: snapshotEntries}
	lis := map[uint64][2]net.Listener{}
	for id := uint64(1); id <= uint64(n); id++ {
		r, h := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
		lis[id] = [2]net.Listener{r, h}
		c.cfg[id] = Peer{RaftAddr: r.Addr().String(), HTTPAddr: h.Addr().String()}
		c.dirs[id] = t.TempDir()
	}
	for id, l := range lis {
		c.startWith(id, l[0], l[1])
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			s.Close()
		}
	})
	return c
}

func (c *testCluster) startWith(id uint64, r, h net.Listener) {
	s, err := Start(Config{ID: id, Cluster: c.cfg, DataDir: c.dirs[id], Tick: 10 * time.Millisecond,
		SnapshotEntries: c.snap}, r, h)
	if err != nil {
		c.t.Fatal(err)
	}
	c.servers[id] = s
}

func (c *testCluster) start(id uint64) {
	c.startWith(id, listen(c.t, c.cfg[id].RaftAddr), listen(c.t, c.cfg[id].HTTPAddr))
}

func (c *testCluster) stop(id uint64) {
	c.servers[id].Close()
	delete(c.servers, id)
}

func (c *testCluster) addrs() []string {
	var out []string
	for _, id := range MemberIDs(c.cfg) {
		out = append(out, c.cfg[id].HTTPAddr)
	}
	return out
}

func (c *testCluster) leader() uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, s := range c.servers {
			if st, err := s.Status(context.Background()); err == nil && st.State == "leader" {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

func (c *testCluster) waitApplied(id uint64, index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if st, err := c.servers[id].Status(context.Background()); err == nil && st.Applied >= index {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("node %d did not apply index %d", id, index)
}

func TestClusterFailover(t *testing.T) {
	c := newTestCluster(t, 3, 20)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl := client.New(c.addrs())
	for i := range 30 {
		if err := cl.Put(ctx, fmt.Sprint("k", i), fmt.Sprint("v", i)); err != nil {
			t.Fatal(err)
		}
	}
	old := c.leader()
	c.stop(old)
	// Writes continue on the surviving majority.
	for i := 30; i < 80; i++ {
		if err := cl.Put(ctx, fmt.Sprint("k", i), fmt.Sprint("v", i)); err != nil {
			t.Fatal(err)
		}
	}
	newLeader := c.leader()
	if newLeader == old {
		t.Fatal("stopped node is still leader")
	}
	// The restarted node recovers from disk and catches up, via snapshot
	// because the leader compacted past its log.
	c.start(old)
	st, _ := c.servers[newLeader].Status(ctx)
	c.waitApplied(old, st.Commit)
	recovered, _ := c.servers[old].Status(ctx)
	if recovered.SnapIndex == 0 {
		t.Fatalf("expected snapshot-based catch-up, status %+v", recovered)
	}
	for i := range 80 {
		if v, err := cl.Get(ctx, fmt.Sprint("k", i)); err != nil || v != fmt.Sprint("v", i) {
			t.Fatalf("k%d = %q, %v", i, v, err)
		}
	}
	// Restart everything: state survives from disk alone.
	for _, id := range MemberIDs(c.cfg) {
		c.stop(id)
	}
	for _, id := range MemberIDs(c.cfg) {
		c.start(id)
	}
	if v, err := cl.Get(ctx, "k79"); err != nil || v != "v79" {
		t.Fatalf("after full restart: %q, %v", v, err)
	}
}

func TestMinorityCannotServe(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	lead := c.leader()
	for _, id := range MemberIDs(c.cfg) {
		if id != lead {
			c.stop(id)
		}
	}
	h := c.servers[lead].Handler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("PUT", "/v1/kv/x", strings.NewReader(`{"value":"y"}`)))
	if w.Code == http.StatusNoContent {
		t.Fatal("write succeeded without a quorum")
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/v1/kv/x", nil))
	if w.Code == http.StatusOK || w.Code == http.StatusNotFound {
		t.Fatalf("read served without a quorum: %d", w.Code)
	}
}

func TestAPIValidationAndRedirect(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	lead := c.leader()
	h := c.servers[lead].Handler()
	cases := []struct {
		method, body string
		header       map[string]string
		status       int
		contains     string
	}{
		{"GET", "", nil, 404, "key not found"},
		{"PUT", `{}`, nil, 400, ""},
		{"PUT", `{"value":null}`, nil, 400, ""},
		{"PUT", `{"value":"bad"} {}`, nil, 400, ""},
		{"PUT", `{"value":"x"}`, map[string]string{HeaderClientID: "c"}, 400, ""},
		{"PUT", `{"value":"hello"}`, nil, 204, ""},
		{"GET", "", nil, 200, `"value":"hello"`},
		{"PUT", `{"value":""}`, map[string]string{HeaderClientID: "c", HeaderSeq: "1"}, 204, ""},
		{"GET", "", nil, 200, `"value":""`},
		{"POST", "", nil, 405, ""},
		{"DELETE", "", nil, 204, ""},
		{"GET", "", nil, 404, ""},
	}
	for _, tc := range cases {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(tc.method, "/v1/kv/test", strings.NewReader(tc.body))
		for k, v := range tc.header {
			r.Header.Set(k, v)
		}
		h.ServeHTTP(w, r)
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.contains) {
			t.Fatalf("%s %s: status=%d body=%s", tc.method, tc.body, w.Code, w.Body.String())
		}
	}
	follower := lead%3 + 1
	w := httptest.NewRecorder()
	c.servers[follower].Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/kv/test", nil))
	if w.Code != http.StatusTemporaryRedirect || !strings.Contains(w.Header().Get("Location"), c.cfg[lead].HTTPAddr) {
		t.Fatalf("follower response %d, Location %q", w.Code, w.Header().Get("Location"))
	}
}

func TestParseCluster(t *testing.T) {
	got, err := ParseCluster("1=a:1/a:2, 2=b:1/b:2")
	if err != nil || len(got) != 2 || got[2] != (Peer{"b:1", "b:2"}) {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"", "1=a", "x=a/b", "1=a/b,1=c/d", "0=a/b"} {
		if _, err := ParseCluster(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}

// TestRealClusterLinearizable records a concurrent client history against
// real processes-in-miniature (gRPC transport, on-disk WAL) while nodes are
// stopped and restarted, then checks it with Porcupine.
func TestRealClusterLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newTestCluster(t, 3, 25)
	start := time.Now()
	var (
		mu      sync.Mutex
		history []porcupine.Operation
	)
	stopAt := start.Add(8 * time.Second)
	var wg sync.WaitGroup
	for ci := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := client.New(c.addrs())
			rng := rand.New(rand.NewSource(int64(ci)))
			for i := 0; time.Now().Before(stopAt); i++ {
				key := string(rune('a' + rng.Intn(3)))
				in := lin.Input{Op: "get", Key: key}
				switch r := rng.Intn(10); {
				case r < 5:
				case r < 9:
					in = lin.Input{Op: "put", Key: key, Value: fmt.Sprintf("%d.%d", ci, i)}
				default:
					in = lin.Input{Op: "delete", Key: key}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				call := time.Since(start).Nanoseconds()
				var out lin.Output
				var err error
				switch in.Op {
				case "get":
					out.Value, err = cl.Get(ctx, key)
					out.Found = err == nil
					if errors.Is(err, client.ErrNotFound) {
						err = nil
					}
				case "put":
					err = cl.Put(ctx, key, in.Value)
				default:
					err = cl.Delete(ctx, key)
				}
				cancel()
				ret := time.Since(start).Nanoseconds()
				if err != nil {
					if in.Op == "get" {
						continue // a failed read has no effect
					}
					ret = math.MaxInt64 // a failed write may still take effect
				}
				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: ci, Input: in, Call: call, Output: out, Return: ret})
				mu.Unlock()
			}
		}()
	}
	// Nemesis: repeatedly stop a random node (often the leader) and restart it.
	rng := rand.New(rand.NewSource(1))
	for time.Now().Before(stopAt.Add(-time.Second)) {
		time.Sleep(time.Duration(300+rng.Intn(500)) * time.Millisecond)
		id := c.leader()
		if rng.Intn(3) == 0 {
			id = uint64(1 + rng.Intn(3))
		}
		c.stop(id)
		time.Sleep(time.Duration(200+rng.Intn(600)) * time.Millisecond)
		c.start(id)
	}
	wg.Wait()
	if len(history) < 100 {
		t.Fatalf("only %d operations completed", len(history))
	}
	result, info := porcupine.CheckOperationsVerbose(lin.Model, history, 30*time.Second)
	if result != porcupine.Ok {
		path := filepath.Join(t.TempDir(), "history.html")
		porcupine.VisualizePath(lin.Model, info, path)
		t.Fatalf("history of %d operations is %s (visualization: %s)", len(history), result, path)
	}
	t.Logf("%d operations linearizable", len(history))
}
