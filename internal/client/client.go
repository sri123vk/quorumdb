// Package client talks to a QuorumDB cluster over HTTP. It follows leader
// redirects, retries through elections and failures, and tags writes with a
// session so that retries apply at most once.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")

type Client struct {
	nodes []string // HTTP host:port of every member
	http  *http.Client
	id    string

	mu     sync.Mutex // one request at a time keeps sequence numbers ordered
	seq    uint64
	leader string
	next   int
	// Retries counts attempts that failed and were retried.
	Retries int
}

func New(nodes []string) *Client {
	b := make([]byte, 8)
	rand.Read(b)
	return &Client{
		nodes: nodes,
		id:    hex.EncodeToString(b),
		http: &http.Client{
			Timeout:       3 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	body, _ := json.Marshal(map[string]string{"value": value})
	_, err := c.do(ctx, http.MethodPut, key, body)
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, http.MethodDelete, key, nil)
	return err
}

// Get performs a linearizable read.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	b, err := c.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return "", err
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return "", err
	}
	return out.Value, nil
}

// Leader returns the address of the last node that served a request.
func (c *Client) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) do(ctx context.Context, method, key string, body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var seq uint64
	if method != http.MethodGet {
		c.seq++
		seq = c.seq
	}
	backoff := 20 * time.Millisecond
	for {
		target := c.leader
		if target == "" {
			target = c.nodes[c.next%len(c.nodes)]
			c.next++
		}
		b, status, location, err := c.attempt(ctx, target, method, key, body, seq)
		switch {
		case err == nil && (status == http.StatusOK || status == http.StatusNoContent):
			c.leader = target
			return b, nil
		case err == nil && status == http.StatusNotFound:
			c.leader = target
			return nil, ErrNotFound
		case err == nil && status == http.StatusBadRequest:
			return nil, fmt.Errorf("bad request: %s", bytes.TrimSpace(b))
		case err == nil && status == http.StatusTemporaryRedirect:
			if u, perr := url.Parse(location); perr == nil && u.Host != "" {
				c.leader = u.Host
				continue
			}
			c.leader = ""
		default:
			c.leader = ""
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s %s: %w (last error: %v, status %d)", method, key, ctx.Err(), err, status)
		}
		c.Retries++
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 500*time.Millisecond)
	}
}

func (c *Client) attempt(ctx context.Context, host, method, key string, body []byte, seq uint64) ([]byte, int, string, error) {
	u := "http://" + host + "/v1/kv/" + url.PathEscape(key)
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, 0, "", err
	}
	if seq != 0 {
		req.Header.Set("X-Client-Id", c.id)
		req.Header.Set("X-Request-Seq", strconv.FormatUint(seq, 10))
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	return b, resp.StatusCode, resp.Header.Get("Location"), err
}
