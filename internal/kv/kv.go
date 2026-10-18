// Package kv is the replicated state machine: a string map plus a client
// session table that makes retried writes apply at most once.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	OpPut    = "put"
	OpDelete = "delete"
)

// Command is a write proposed through Raft. ClientID and Seq identify the
// request; a client issues one request at a time with increasing Seq and
// retries with the same Seq until it gets a response.
type Command struct {
	ClientID string `json:"c,omitempty"`
	Seq      uint64 `json:"s,omitempty"`
	Op       string `json:"op"`
	Key      string `json:"k"`
	Value    string `json:"v,omitempty"`
}

func (c Command) Validate() error {
	if c.Key == "" {
		return errors.New("key must not be empty")
	}
	if c.Op != OpPut && c.Op != OpDelete {
		return fmt.Errorf("unknown op %q", c.Op)
	}
	if (c.ClientID == "") != (c.Seq == 0) {
		return errors.New("client ID and sequence must be set together")
	}
	return nil
}

func Encode(c Command) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // Command contains only strings and integers.
	}
	return b
}

// Store is not safe for concurrent use; the node's event loop owns it.
type Store struct {
	values   map[string]string
	sessions map[string]uint64
}

func New() *Store {
	return &Store{values: map[string]string{}, sessions: map[string]uint64{}}
}

// Apply executes a committed command. Every replica applies the same log,
// so invalid commands are skipped identically everywhere. It reports
// whether the command was a duplicate of an already applied request.
func (s *Store) Apply(data []byte) (duplicate bool, err error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return false, fmt.Errorf("decode command: %w", err)
	}
	if err := c.Validate(); err != nil {
		return false, err
	}
	if c.ClientID != "" {
		if c.Seq <= s.sessions[c.ClientID] {
			return true, nil
		}
		s.sessions[c.ClientID] = c.Seq
	}
	if c.Op == OpPut {
		s.values[c.Key] = c.Value
	} else {
		delete(s.values, c.Key)
	}
	return false, nil
}

func (s *Store) Get(key string) (string, bool) {
	v, ok := s.values[key]
	return v, ok
}

func (s *Store) Len() int { return len(s.values) }

type image struct {
	Values   map[string]string `json:"values"`
	Sessions map[string]uint64 `json:"sessions"`
}

func (s *Store) Snapshot() []byte {
	b, err := json.Marshal(image{s.values, s.sessions})
	if err != nil {
		panic(err)
	}
	return b
}

func (s *Store) Restore(data []byte) error {
	var img image
	if err := json.Unmarshal(data, &img); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if img.Values == nil {
		img.Values = map[string]string{}
	}
	if img.Sessions == nil {
		img.Sessions = map[string]uint64{}
	}
	s.values, s.sessions = img.Values, img.Sessions
	return nil
}
