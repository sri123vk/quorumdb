// Package wal persists Raft state in a data directory:
//
//	LOCK      flock held while the directory is open
//	state     current term and vote, replaced atomically
//	snapshot  latest snapshot, replaced atomically
//	wal.log   header naming the snapshot it follows, then log entries
//
// Every file is checksummed. Save and Compact return only after fsync.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"quorumdb/internal/raft"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

const maxRecord = 64 << 20

// WAL is not safe for concurrent use.
type WAL struct {
	dir   string
	lock  *os.File
	log   *os.File
	state raft.HardState
	// base is the snapshot the log follows; offsets[i] is the file offset
	// of the record for entry base.Index+1+i.
	base    raft.Snapshot
	offsets []int64
	size    int64
}

// Open locks dir, creating it if needed, and loads its contents.
func Open(dir string) (*WAL, raft.HardState, *raft.Snapshot, []raft.Entry, error) {
	var hs raft.HardState
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, hs, nil, nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, hs, nil, nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, hs, nil, nil, fmt.Errorf("data directory already in use: %w", err)
	}
	w := &WAL{dir: dir, lock: lock}
	snap, ents, err := w.load()
	if err != nil {
		w.Close()
		return nil, hs, nil, nil, err
	}
	return w, w.state, snap, ents, nil
}

func (w *WAL) load() (*raft.Snapshot, []raft.Entry, error) {
	if b, err := readChecked(filepath.Join(w.dir, "state")); err == nil {
		if len(b) != 16 {
			return nil, nil, errors.New("state file has wrong length")
		}
		w.state = raft.HardState{Term: binary.BigEndian.Uint64(b), Vote: binary.BigEndian.Uint64(b[8:])}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	var snap *raft.Snapshot
	if b, err := readChecked(filepath.Join(w.dir, "snapshot")); err == nil {
		if len(b) < 16 {
			return nil, nil, errors.New("snapshot file too short")
		}
		snap = &raft.Snapshot{Index: binary.BigEndian.Uint64(b), Term: binary.BigEndian.Uint64(b[8:]), Data: b[16:]}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	path := filepath.Join(w.dir, "wal.log")
	base, ents, offsets, size, err := readLog(path)
	if errors.Is(err, os.ErrNotExist) {
		return snap, nil, w.rewrite(snapOrZero(snap), nil)
	}
	if err != nil {
		return nil, nil, err
	}
	if snap == nil && base.Index != 0 {
		return nil, nil, errors.New("log follows a snapshot that is missing")
	}
	if snap != nil && (base.Index != snap.Index || base.Term != snap.Term) {
		// A crash interrupted Save or Compact after the snapshot was
		// replaced but before the log was rewritten.
		if base.Index > snap.Index {
			return nil, nil, errors.New("log is newer than snapshot")
		}
		var kept []raft.Entry
		for i, e := range ents {
			if e.Index == snap.Index && e.Term == snap.Term {
				kept = ents[i+1:]
				break
			}
		}
		return snap, kept, w.rewrite(*snap, kept)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, nil, err
	}
	if err := f.Truncate(size); err != nil { // drop a torn tail
		f.Close()
		return nil, nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, nil, err
	}
	w.log, w.base, w.offsets, w.size = f, base, offsets, size
	return snap, ents, nil
}

func snapOrZero(s *raft.Snapshot) raft.Snapshot {
	if s == nil {
		return raft.Snapshot{}
	}
	return *s
}

// readLog parses wal.log. It returns the size of the valid prefix; an
// incomplete or checksum-failing final record is a torn write and is
// excluded, while damage before the final record is an error.
func readLog(path string) (raft.Snapshot, []raft.Entry, []int64, int64, error) {
	var base raft.Snapshot
	f, err := os.Open(path)
	if err != nil {
		return base, nil, nil, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return base, nil, nil, 0, err
	}
	r := bufio.NewReader(f)
	var (
		ents    []raft.Entry
		offsets []int64
		offset  int64
		header  = true
	)
	for {
		payload, n, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			if offset+int64(n) >= info.Size() && !header {
				break // torn final record
			}
			return base, nil, nil, 0, fmt.Errorf("corrupt log at byte %d: %w", offset, err)
		}
		if header {
			if len(payload) != 16 {
				return base, nil, nil, 0, errors.New("corrupt log header")
			}
			base = raft.Snapshot{Index: binary.BigEndian.Uint64(payload), Term: binary.BigEndian.Uint64(payload[8:])}
			header = false
		} else {
			e, err := decodeEntry(payload)
			if err != nil {
				return base, nil, nil, 0, fmt.Errorf("corrupt log at byte %d: %w", offset, err)
			}
			if want := base.Index + 1 + uint64(len(ents)); e.Index != want {
				return base, nil, nil, 0, fmt.Errorf("log entry %d at byte %d, want %d", e.Index, offset, want)
			}
			ents = append(ents, e)
			offsets = append(offsets, offset)
		}
		offset += int64(n)
	}
	if header {
		return base, nil, nil, 0, errors.New("log header missing")
	}
	return base, ents, offsets, offset, nil
}

var errChecksum = errors.New("checksum mismatch")

// readRecord reads one length-and-CRC framed record, returning the number
// of bytes it occupied (or that were available for a partial record).
func readRecord(r *bufio.Reader) ([]byte, int, error) {
	var hdr [8]byte
	n, err := io.ReadFull(r, hdr[:])
	if err == io.EOF {
		return nil, 0, io.EOF
	}
	if err != nil {
		return nil, n, io.ErrUnexpectedEOF
	}
	size := binary.BigEndian.Uint32(hdr[:])
	if size > maxRecord {
		return nil, n, fmt.Errorf("record length %d too large", size)
	}
	payload := make([]byte, size)
	m, err := io.ReadFull(r, payload)
	if err != nil {
		return nil, n + m, io.ErrUnexpectedEOF
	}
	if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(hdr[4:]) {
		return nil, n + m, errChecksum
	}
	return payload, n + m, nil
}

func frame(payload []byte) []byte {
	b := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(b, uint32(len(payload)))
	binary.BigEndian.PutUint32(b[4:], crc32.Checksum(payload, crcTable))
	return append(b, payload...)
}

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 17, 17+len(e.Data))
	binary.BigEndian.PutUint64(b, e.Term)
	binary.BigEndian.PutUint64(b[8:], e.Index)
	b[16] = byte(e.Type)
	return frame(append(b, e.Data...))
}

func decodeEntry(b []byte) (raft.Entry, error) {
	if len(b) < 17 {
		return raft.Entry{}, errors.New("short entry")
	}
	return raft.Entry{
		Term:  binary.BigEndian.Uint64(b),
		Index: binary.BigEndian.Uint64(b[8:]),
		Type:  raft.EntryType(b[16]),
		Data:  append([]byte(nil), b[17:]...),
	}, nil
}

func header(base raft.Snapshot) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, base.Index)
	binary.BigEndian.PutUint64(b[8:], base.Term)
	return frame(b)
}

func (w *WAL) LastIndex() uint64 { return w.base.Index + uint64(len(w.offsets)) }

// Save durably records a Ready: its hard state, a snapshot received from
// the leader (which replaces the log), and new entries, which replace any
// persisted entries at or after ents[0].Index.
func (w *WAL) Save(hs raft.HardState, snap *raft.Snapshot, ents []raft.Entry) error {
	if hs != w.state {
		b := make([]byte, 16)
		binary.BigEndian.PutUint64(b, hs.Term)
		binary.BigEndian.PutUint64(b[8:], hs.Vote)
		if err := writeChecked(w.dir, "state", b); err != nil {
			return err
		}
		w.state = hs
	}
	if snap != nil {
		if err := w.writeSnapshot(*snap); err != nil {
			return err
		}
		return w.rewrite(*snap, ents)
	}
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].Index
	if first <= w.base.Index || first > w.LastIndex()+1 {
		return fmt.Errorf("wal: entry %d does not follow log (%d, %d]", first, w.base.Index, w.LastIndex())
	}
	if first <= w.LastIndex() {
		keep := first - w.base.Index - 1
		if err := w.log.Truncate(w.offsets[keep]); err != nil {
			return err
		}
		w.size = w.offsets[keep]
		w.offsets = w.offsets[:keep]
	}
	var buf []byte
	for _, e := range ents {
		w.offsets = append(w.offsets, w.size+int64(len(buf)))
		buf = append(buf, encodeEntry(e)...)
	}
	if _, err := w.log.Write(buf); err != nil {
		return err
	}
	w.size += int64(len(buf))
	return w.log.Sync()
}

// Compact durably records a local snapshot and rewrites the log to hold
// only rest, the entries after it.
func (w *WAL) Compact(snap raft.Snapshot, rest []raft.Entry) error {
	if err := w.writeSnapshot(snap); err != nil {
		return err
	}
	return w.rewrite(snap, rest)
}

func (w *WAL) writeSnapshot(s raft.Snapshot) error {
	b := make([]byte, 16, 16+len(s.Data))
	binary.BigEndian.PutUint64(b, s.Index)
	binary.BigEndian.PutUint64(b[8:], s.Term)
	return writeChecked(w.dir, "snapshot", append(b, s.Data...))
}

// rewrite atomically replaces wal.log with one following base.
func (w *WAL) rewrite(base raft.Snapshot, ents []raft.Entry) error {
	buf := header(base)
	offsets := make([]int64, 0, len(ents))
	for i, e := range ents {
		if e.Index != base.Index+1+uint64(i) {
			return fmt.Errorf("wal: entry %d does not follow snapshot %d", e.Index, base.Index)
		}
		offsets = append(offsets, int64(len(buf)))
		buf = append(buf, encodeEntry(e)...)
	}
	path := filepath.Join(w.dir, "wal.log")
	if err := writeAtomic(w.dir, "wal.log", buf); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if w.log != nil {
		w.log.Close()
	}
	w.log, w.base, w.offsets, w.size = f, base, offsets, int64(len(buf))
	return nil
}

func (w *WAL) Close() error {
	var err error
	if w.log != nil {
		err = w.log.Close()
		w.log = nil
	}
	if w.lock != nil {
		w.lock.Close() // releases the flock
		w.lock = nil
	}
	return err
}

// writeChecked atomically replaces dir/name with data plus a CRC trailer.
func writeChecked(dir, name string, data []byte) error {
	b := binary.BigEndian.AppendUint32(append([]byte(nil), data...), crc32.Checksum(data, crcTable))
	return writeAtomic(dir, name, b)
}

func readChecked(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("%s: too short", filepath.Base(path))
	}
	data, sum := b[:len(b)-4], binary.BigEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(data, crcTable) != sum {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), errChecksum)
	}
	return data, nil
}

func writeAtomic(dir, name string, data []byte) error {
	tmp, err := os.CreateTemp(dir, name+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
