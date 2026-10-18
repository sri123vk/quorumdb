// Package server runs one QuorumDB replica: a single event-loop goroutine
// owns the Raft node, a gRPC transport carries peer traffic, and an HTTP
// API serves clients.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"quorumdb/internal/kv"
	"quorumdb/internal/node"
	"quorumdb/internal/raft"
	"quorumdb/internal/transport"
	"quorumdb/internal/wal"
)

type Peer struct {
	RaftAddr string
	HTTPAddr string
}

type Config struct {
	ID      uint64
	Cluster map[uint64]Peer // every member, including ID
	DataDir string
	// Tick is the Raft heartbeat interval; elections time out after
	// ElectionTicks to twice that many ticks.
	Tick            time.Duration
	ElectionTicks   int
	SnapshotEntries uint64
	RequestTimeout  time.Duration
}

func (c *Config) setDefaults() {
	if c.Tick == 0 {
		c.Tick = 50 * time.Millisecond
	}
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = 10000
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
}

type Server struct {
	cfg       Config
	wal       *wal.WAL
	node      *node.Node
	transport *transport.Transport
	http      *http.Server

	inbox chan raft.Message
	calls chan func()
	stop  chan struct{}
	done  chan struct{}
	err   error
}

// Start opens the data directory and serves Raft traffic on raftLis and
// the client API on httpLis.
func Start(cfg Config, raftLis, httpLis net.Listener) (*Server, error) {
	cfg.setDefaults()
	if _, ok := cfg.Cluster[cfg.ID]; !ok {
		return nil, fmt.Errorf("node %d is not in the cluster configuration", cfg.ID)
	}
	w, hs, snap, ents, err := wal.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, wal: w, inbox: make(chan raft.Message, 4096), calls: make(chan func(), 1024),
		stop: make(chan struct{}), done: make(chan struct{})}
	peers := map[uint64]string{}
	var ids []uint64
	for id, p := range cfg.Cluster {
		peers[id] = p.RaftAddr
		ids = append(ids, id)
	}
	s.transport = transport.New(cfg.ID, peers, func(m raft.Message) {
		select {
		case s.inbox <- m:
		default: // overloaded; Raft retries
		}
	})
	s.node, err = node.New(node.Config{
		Raft: raft.Config{ID: cfg.ID, Peers: ids, ElectionTick: cfg.ElectionTicks, HeartbeatTick: 1,
			MaxEntriesPerMsg: 256, MaxUncommitted: 4096, Seed: time.Now().UnixNano() ^ int64(cfg.ID)},
		SnapshotEntries: cfg.SnapshotEntries,
	}, w, hs, snap, ents, s.transport.Send)
	if err != nil {
		s.transport.Close()
		w.Close()
		return nil, err
	}
	s.http = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: cfg.RequestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second}
	go s.transport.Serve(raftLis)
	go s.http.Serve(httpLis)
	go s.run()
	return s, nil
}

func (s *Server) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			s.node.Stop()
			return
		case <-ticker.C:
			s.node.Tick()
		case m := <-s.inbox:
			s.node.Step(m)
		case fn := <-s.calls:
			fn()
		}
		// Fold whatever else is already waiting into the same fsync.
	batch:
		for range 512 {
			select {
			case m := <-s.inbox:
				s.node.Step(m)
			case fn := <-s.calls:
				fn()
			default:
				break batch
			}
		}
		s.node.Process()
		if err := s.node.Err(); err != nil {
			s.err = err
			log.Printf("node %d halted: %v", s.cfg.ID, err)
			return
		}
	}
}

// Done is closed when the replica stops, after Close or a storage failure.
func (s *Server) Done() <-chan struct{} { return s.done }

// Err reports why the replica halted, if it failed.
func (s *Server) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.http.Shutdown(ctx)
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
	s.transport.Close()
	return s.wal.Close()
}

// call runs fn on the event loop.
func (s *Server) call(ctx context.Context, fn func()) error {
	select {
	case s.calls <- fn:
		return nil
	case <-s.done:
		return node.ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Status struct {
	ID        uint64 `json:"id"`
	Term      uint64 `json:"term"`
	State     string `json:"state"`
	Leader    uint64 `json:"leader"`
	Commit    uint64 `json:"commit"`
	Applied   uint64 `json:"applied"`
	LastIndex uint64 `json:"last_index"`
	SnapIndex uint64 `json:"snapshot_index"`
	Keys      int    `json:"keys"`
}

func (s *Server) Status(ctx context.Context) (Status, error) {
	res := make(chan Status, 1)
	err := s.call(ctx, func() {
		st := s.node.Status()
		res <- Status{ID: st.ID, Term: st.Term, State: st.State.String(), Leader: st.Lead,
			Commit: st.Commit, Applied: st.Applied, LastIndex: st.LastIndex, SnapIndex: st.SnapIndex,
			Keys: s.node.Len()}
	})
	if err != nil {
		return Status{}, err
	}
	select {
	case st := <-res:
		return st, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()
	}
}

// Client request headers. A client that sets both gets exactly-once writes
// across retries; it must send one request at a time with increasing
// sequence numbers.
const (
	HeaderClientID = "X-Client-Id"
	HeaderSeq      = "X-Request-Seq"
	HeaderLeader   = "X-Raft-Leader"
)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		st, err := s.Status(r.Context())
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, st)
	})
	mux.HandleFunc("/v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		switch r.Method {
		case http.MethodGet:
			s.serveGet(w, r, key)
		case http.MethodPut:
			r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
			var body struct {
				Value *string `json:"value"`
			}
			d := json.NewDecoder(r.Body)
			d.DisallowUnknownFields()
			if err := d.Decode(&body); err != nil || body.Value == nil {
				http.Error(w, "expected JSON with a string value", http.StatusBadRequest)
				return
			}
			if err := d.Decode(new(any)); err != io.EOF {
				http.Error(w, "expected one JSON object", http.StatusBadRequest)
				return
			}
			s.serveWrite(w, r, kv.Command{Op: kv.OpPut, Key: key, Value: *body.Value})
		case http.MethodDelete:
			s.serveWrite(w, r, kv.Command{Op: kv.OpDelete, Key: key})
		default:
			w.Header().Set("Allow", "GET, PUT, DELETE")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	return mux
}

func (s *Server) serveWrite(w http.ResponseWriter, r *http.Request, cmd kv.Command) {
	if id := r.Header.Get(HeaderClientID); id != "" {
		seq, err := strconv.ParseUint(r.Header.Get(HeaderSeq), 10, 64)
		if err != nil || seq == 0 {
			http.Error(w, HeaderSeq+" must be a positive integer with "+HeaderClientID, http.StatusBadRequest)
			return
		}
		cmd.ClientID, cmd.Seq = id, seq
	}
	if err := cmd.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	res := make(chan error, 1)
	err := s.call(ctx, func() { s.node.Write(cmd, func(err error) { res <- err }) })
	if err == nil {
		select {
		case err = <-res:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serveGet(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	type result struct {
		value string
		found bool
		err   error
	}
	res := make(chan result, 1)
	err := s.call(ctx, func() {
		s.node.Read(key, func(v string, found bool, err error) { res <- result{v, found, err} })
	})
	var out result
	if err == nil {
		select {
		case out = <-res:
		case <-ctx.Done():
			out.err = ctx.Err()
		}
	} else {
		out.err = err
	}
	switch {
	case out.err != nil:
		s.writeError(w, r, out.err)
	case !out.found:
		http.Error(w, "key not found", http.StatusNotFound)
	default:
		writeJSON(w, map[string]string{"value": out.value})
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var nl *node.NotLeaderError
	if errors.As(err, &nl) {
		if p, ok := s.cfg.Cluster[nl.Leader]; ok && nl.Leader != s.cfg.ID {
			w.Header().Set(HeaderLeader, strconv.FormatUint(nl.Leader, 10))
			http.Redirect(w, r, "http://"+p.HTTPAddr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, "no leader elected; retry", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Retry-After", "1")
	switch {
	case errors.Is(err, node.ErrUnknownOutcome), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "outcome unknown; retry with the same "+HeaderSeq, http.StatusServiceUnavailable)
	case errors.Is(err, node.ErrBusy):
		http.Error(w, "too many pending writes; retry", http.StatusServiceUnavailable)
	default:
		http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// ParseCluster parses "1=raftAddr/httpAddr,2=...".
func ParseCluster(spec string) (map[uint64]Peer, error) {
	out := map[uint64]Peer{}
	for _, part := range strings.Split(spec, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		idStr, addrs, ok := strings.Cut(part, "=")
		raftAddr, httpAddr, ok2 := strings.Cut(addrs, "/")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if !ok || !ok2 || err != nil || id == 0 || raftAddr == "" || httpAddr == "" {
			return nil, fmt.Errorf("invalid cluster member %q; want id=raftAddr/httpAddr", part)
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("duplicate member %d", id)
		}
		out[id] = Peer{RaftAddr: raftAddr, HTTPAddr: httpAddr}
	}
	if len(out) == 0 {
		return nil, errors.New("empty cluster")
	}
	return out, nil
}

// MemberIDs returns the sorted member IDs.
func MemberIDs(c map[uint64]Peer) []uint64 {
	ids := make([]uint64, 0, len(c))
	for id := range c {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
