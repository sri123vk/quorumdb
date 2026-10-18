// Command qdbctl is a QuorumDB client that follows leader changes.
//
//	qdbctl -nodes 127.0.0.1:8001,127.0.0.1:8002 put KEY VALUE
//	qdbctl -nodes ... get KEY
//	qdbctl -nodes ... delete KEY
//	qdbctl -nodes ... write-loop -n 200 -prefix demo
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"quorumdb/internal/client"
)

func main() {
	nodes := flag.String("nodes", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "comma-separated HTTP addresses")
	timeout := flag.Duration("timeout", 30*time.Second, "per-operation timeout, including retries")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: qdbctl [-nodes a,b,c] put KEY VALUE | get KEY | delete KEY | write-loop [-n N] [-prefix P] [-interval D]")
	}
	flag.Parse()
	c := client.New(strings.Split(*nodes, ","))
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), *timeout)
	}
	var err error
	switch {
	case args[0] == "put" && len(args) == 3:
		cx, cancel := ctx()
		err = c.Put(cx, args[1], args[2])
		cancel()
	case args[0] == "get" && len(args) == 2:
		cx, cancel := ctx()
		var v string
		v, err = c.Get(cx, args[1])
		cancel()
		if err == nil {
			fmt.Println(v)
		}
	case args[0] == "delete" && len(args) == 2:
		cx, cancel := ctx()
		err = c.Delete(cx, args[1])
		cancel()
	case args[0] == "write-loop":
		err = writeLoop(c, args[1:], *timeout)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if errors.Is(err, client.ErrNotFound) {
		fmt.Fprintln(os.Stderr, "not found")
		os.Exit(1)
	}
	if err != nil {
		log.Fatal(err)
	}
}

// writeLoop writes keys sequentially, reporting leader changes and the
// slowest write, which shows how long a failover stalls clients.
func writeLoop(c *client.Client, args []string, timeout time.Duration) error {
	fs := flag.NewFlagSet("write-loop", flag.ExitOnError)
	n := fs.Int("n", 100, "number of writes")
	prefix := fs.String("prefix", "key", "key prefix")
	interval := fs.Duration("interval", 50*time.Millisecond, "pause between writes")
	fs.Parse(args)
	var slowest time.Duration
	leader := ""
	for i := range *n {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		err := c.Put(ctx, fmt.Sprintf("%s-%d", *prefix, i), fmt.Sprint(i))
		cancel()
		if err != nil {
			return fmt.Errorf("write %d: %w", i, err)
		}
		d := time.Since(start)
		slowest = max(slowest, d)
		if l := c.Leader(); l != leader {
			fmt.Printf("write %d: served by %s (took %v)\n", i, l, d.Round(time.Millisecond))
			leader = l
		}
		time.Sleep(*interval)
	}
	fmt.Printf("%d writes acknowledged; %d retries; slowest write %v\n", *n, c.Retries, slowest.Round(time.Millisecond))
	return nil
}
