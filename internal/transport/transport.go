// Package transport carries Raft messages between nodes over gRPC. Each
// node holds one client stream to every peer. Delivery is best effort:
// messages are dropped when a peer is unreachable or its queue is full, and
// Raft recovers by retrying.
package transport

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"quorumdb/internal/raft"
	"quorumdb/internal/raftpb"
)

const (
	queueSize   = 4096
	maxBatch    = 128
	maxMsgBytes = 256 << 20 // snapshots travel in one message
)

type Transport struct {
	raftpb.UnimplementedRaftServer
	id      uint64
	recv    func(raft.Message)
	server  *grpc.Server
	senders map[uint64]*sender
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

// New creates a transport for node id. peers maps every other node ID to
// its Raft address; recv is called for each inbound message from gRPC
// goroutines.
func New(id uint64, peers map[uint64]string, recv func(raft.Message)) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{id: id, recv: recv, senders: map[uint64]*sender{}, cancel: cancel,
		server: grpc.NewServer(grpc.MaxRecvMsgSize(maxMsgBytes))}
	raftpb.RegisterRaftServer(t.server, t)
	for peer, addr := range peers {
		if peer == id {
			continue
		}
		s := &sender{peer: peer, addr: addr, queue: make(chan raft.Message, queueSize)}
		t.senders[peer] = s
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			s.run(ctx)
		}()
	}
	return t
}

// Serve accepts peer streams on l until Close.
func (t *Transport) Serve(l net.Listener) error { return t.server.Serve(l) }

// Send queues messages without blocking.
func (t *Transport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if s := t.senders[m.To]; s != nil {
			select {
			case s.queue <- m:
			default: // peer is too far behind; Raft will resend
			}
		}
	}
}

func (t *Transport) Close() {
	t.cancel()
	t.server.Stop()
	t.wg.Wait()
}

// Stream receives batches from one peer.
func (t *Transport) Stream(stream grpc.ClientStreamingServer[raftpb.Batch, raftpb.Ack]) error {
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&raftpb.Ack{})
		}
		if err != nil {
			return err
		}
		for _, m := range b.Messages {
			if m.To == t.id && (m.Type != raftpb.MessageType_MSG_SNAP || m.Snapshot != nil) {
				t.recv(FromProto(m))
			}
		}
	}
}

type sender struct {
	peer  uint64
	addr  string
	queue chan raft.Message
}

func (s *sender) run(ctx context.Context) {
	conn, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgBytes)))
	if err != nil {
		log.Printf("transport: peer %d: %v", s.peer, err)
		return
	}
	defer conn.Close()
	client := raftpb.NewRaftClient(conn)
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := client.Stream(ctx)
		if err == nil {
			err = s.pump(ctx, stream)
			backoff = 50 * time.Millisecond
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
		s.drain() // messages queued while disconnected are stale
	}
}

// pump forwards queued messages until the stream fails.
func (s *sender) pump(ctx context.Context, stream grpc.ClientStreamingClient[raftpb.Batch, raftpb.Ack]) error {
	for {
		var first raft.Message
		select {
		case <-ctx.Done():
			return ctx.Err()
		case first = <-s.queue:
		}
		b := &raftpb.Batch{Messages: []*raftpb.Message{ToProto(first)}}
	more:
		for len(b.Messages) < maxBatch {
			select {
			case m := <-s.queue:
				b.Messages = append(b.Messages, ToProto(m))
			default:
				break more
			}
		}
		if err := stream.Send(b); err != nil {
			return err
		}
	}
}

func (s *sender) drain() {
	for {
		select {
		case <-s.queue:
		default:
			return
		}
	}
}

func ToProto(m raft.Message) *raftpb.Message {
	pm := &raftpb.Message{
		Type: raftpb.MessageType(m.Type), From: m.From, To: m.To, Term: m.Term, LogTerm: m.LogTerm,
		Index: m.Index, Commit: m.Commit, Reject: m.Reject, Hint: m.Hint, Context: m.Context,
	}
	for _, e := range m.Entries {
		pm.Entries = append(pm.Entries, &raftpb.Entry{Term: e.Term, Index: e.Index,
			Type: raftpb.EntryType(e.Type), Data: e.Data})
	}
	if s := m.Snapshot; s != nil {
		pm.Snapshot = &raftpb.Snapshot{Index: s.Index, Term: s.Term, Data: s.Data}
	}
	return pm
}

func FromProto(pm *raftpb.Message) raft.Message {
	m := raft.Message{
		Type: raft.MessageType(pm.Type), From: pm.From, To: pm.To, Term: pm.Term, LogTerm: pm.LogTerm,
		Index: pm.Index, Commit: pm.Commit, Reject: pm.Reject, Hint: pm.Hint, Context: pm.Context,
	}
	for _, e := range pm.Entries {
		m.Entries = append(m.Entries, raft.Entry{Term: e.Term, Index: e.Index,
			Type: raft.EntryType(e.Type), Data: e.Data})
	}
	if s := pm.Snapshot; s != nil {
		m.Snapshot = &raft.Snapshot{Index: s.Index, Term: s.Term, Data: s.Data}
	}
	return m
}
