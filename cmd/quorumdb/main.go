package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"quorumdb/internal/server"
)

func main() {
	id := flag.Uint64("id", 1, "this node's ID in -cluster")
	cluster := flag.String("cluster", "1=127.0.0.1:9001/127.0.0.1:8001",
		"comma-separated members as id=raftAddr/httpAddr")
	dir := flag.String("data", "", "exclusive data directory (default data/node<id>)")
	tick := flag.Duration("tick", 50*time.Millisecond, "Raft heartbeat interval; elections time out after 10-20 ticks")
	snapshot := flag.Uint64("snapshot-entries", 10000, "compact the log after this many applied entries")
	flag.Parse()
	if err := run(*id, *cluster, *dir, *tick, *snapshot); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(id uint64, spec, dir string, tick time.Duration, snapshot uint64) error {
	members, err := server.ParseCluster(spec)
	if err != nil {
		return err
	}
	self, ok := members[id]
	if !ok {
		return fmt.Errorf("node %d is not in -cluster", id)
	}
	if dir == "" {
		dir = fmt.Sprintf("data/node%d", id)
	}
	raftLis, err := net.Listen("tcp", self.RaftAddr)
	if err != nil {
		return err
	}
	httpLis, err := net.Listen("tcp", self.HTTPAddr)
	if err != nil {
		raftLis.Close()
		return err
	}
	s, err := server.Start(server.Config{ID: id, Cluster: members, DataDir: dir, Tick: tick,
		SnapshotEntries: snapshot}, raftLis, httpLis)
	if err != nil {
		return err
	}
	log.Printf("QuorumDB node %d: raft=%s http=%s data=%s members=%d", id, self.RaftAddr, self.HTTPAddr, dir, len(members))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		return s.Close()
	case <-s.Done():
		err := s.Err()
		s.Close()
		return err
	}
}
