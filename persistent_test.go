package monotime

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func newTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.wal")
}

func readLastEntry(t *testing.T, filename string, entrySize int) []byte {
	t.Helper()
	b, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		t.Fatal(err)
	}
	if len(b) < entrySize {
		return nil
	}
	lastFull := (len(b) / entrySize) * entrySize
	if lastFull == 0 {
		return nil
	}
	return b[lastFull-entrySize : lastFull]
}

func TestPersistentGenRoundtrip(t *testing.T) {
	filename := newTestDir(t)

	d1, err := OpenGen(filename)
	if err != nil {
		t.Fatalf("OpenGen (1): %v", err)
	}

	_, err = d1.Next()
	if err != nil {
		t.Fatalf("d1.Next() 1: %v", err)
	}
	v2, err := d1.Next()
	if err != nil {
		t.Fatalf("d1.Next() 2: %v", err)
	}
	if err = d1.Close(); err != nil {
		t.Fatalf("d1.Close(): %v", err)
	}

	lastB := readLastEntry(t, filename, 8)
	if lastB == nil {
		t.Fatal("WAL file is empty after close")
	}
	lastV := int64(binary.BigEndian.Uint64(lastB))
	if lastV != v2 {
		t.Fatalf("WAL value mismatch: expected %d, got %d", v2, lastV)
	}

	d2, err := OpenGen(filename)
	if err != nil {
		t.Fatalf("OpenGen (2): %v", err)
	}
	if d2.gen.last.Load() < v2 {
		t.Fatalf("Generator did not load last value: expected >= %d, got %d", v2, d2.gen.last.Load())
	}

	v3, err := d2.Next()
	if err != nil {
		t.Fatalf("d2.Next(): %v", err)
	}

	if v3 <= v2 {
		t.Fatalf("Monotonicity violated on reopen: v3 (%d) <= v2 (%d)", v3, v2)
	}
	if err = d2.Close(); err != nil {
		t.Fatalf("d2.Close(): %v", err)
	}
}

func TestPersistentGenUUIDRoundtrip(t *testing.T) {
	filename := newTestDir(t)
	nodeID := 123

	d1, err := OpenGenUUID(nodeID, filename)
	if err != nil {
		t.Fatalf("OpenGenUUID (1): %v", err)
	}

	_, err = d1.Next()
	if err != nil {
		t.Fatalf("d1.Next() 1: %v", err)
	}
	u2, err := d1.Next()
	if err != nil {
		t.Fatalf("d1.Next() 2: %v", err)
	}
	if err = d1.Close(); err != nil {
		t.Fatalf("d1.Close(): %v", err)
	}

	lastB := readLastEntry(t, filename, 16)
	if lastB == nil {
		t.Fatal("WAL file is empty after close")
	}
	if !bytes.Equal(lastB, u2[:]) {
		t.Fatalf("WAL value mismatch: expected %s, got %s", u2, UUID(lastB))
	}

	d2, err := OpenGenUUID(nodeID, filename)
	if err != nil {
		t.Fatalf("OpenGenUUID (2): %v", err)
	}

	if d2.gen.lastNano.Load() < u2.Time().UnixNano() {
		t.Fatalf("Generator did not load last value: expected >= %d, got %d", u2.Time().UnixNano(), d2.gen.lastNano.Load())
	}

	u3, err := d2.Next()
	if err != nil {
		t.Fatalf("d2.Next(): %v", err)
	}

	if u3.Time().UnixNano() <= u2.Time().UnixNano() {
		t.Fatalf("Monotonicity violated on reopen: u3 (%d) <= u2 (%d)", u3.Time().UnixNano(), u2.Time().UnixNano())
	}
	if err = d2.Close(); err != nil {
		t.Fatalf("d2.Close(): %v", err)
	}
}

func TestPersistentGenUUIDNodeIDMismatch(t *testing.T) {
	filename := newTestDir(t)

	d1, err := OpenGenUUID(10, filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d1.Next(); err != nil {
		t.Fatal(err)
	}
	if err = d1.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = OpenGenUUID(20, filename)
	if err == nil {
		t.Fatal("Expected error when opening WAL with mismatched nodeID, but got nil")
	}
}

func TestWALRotation(t *testing.T) {
	filename := newTestDir(t)
	entrySize := 8
	maxEntries := 5

	w, last, err := walOpen(filename, entrySize, maxEntries)
	if err != nil {
		t.Fatalf("walOpen: %v", err)
	}
	if len(last) != 0 {
		t.Fatal("lastEntry should be nil on first open")
	}

	entry := make([]byte, entrySize)

	for i := 0; i < maxEntries; i++ {
		binary.BigEndian.PutUint64(entry, uint64(i+1))
		if err = w.store(entry); err != nil {
			t.Fatalf("store() %d: %v", i, err)
		}
	}

	if w.count != maxEntries {
		t.Fatalf("count mismatch: expected %d, got %d", maxEntries, w.count)
	}

	entry6 := make([]byte, entrySize)
	binary.BigEndian.PutUint64(entry6, uint64(6))
	if err = w.store(entry6); err != nil {
		t.Fatalf("store() 6 (rotate): %v", err)
	}
	if w.count != 0 {
		t.Fatalf("count after rotate: expected 0, got %d", w.count)
	}
	if err = w.close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	b, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != entrySize {
		t.Fatalf("WAL file has wrong size after rotate: expected %d, got %d", entrySize, len(b))
	}
	if !bytes.Equal(b, entry6) {
		t.Fatalf("WAL file has wrong content after rotate: expected %v, got %v", entry6, b)
	}
}

func TestWALStoreClosed(t *testing.T) {
	filename := newTestDir(t)
	w, _, err := walOpen(filename, 8, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.close(); err != nil {
		t.Fatal(err)
	}
	err = w.store(make([]byte, 8))
	if err == nil {
		t.Fatal("store() on closed WAL should return error, got nil")
	}
}

func newBenchPersistentGen(b *testing.B) (*PersistentGen, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "mono_time_bench_*")
	if err != nil {
		b.Fatalf("MkdirTemp: %v", err)
	}
	filename := filepath.Join(dir, "bench.wal")

	d, err := OpenGen(filename)
	if err != nil {
		b.Fatalf("OpenGen: %v", err)
	}

	cleanup := func() {
		_ = d.Close()
		_ = os.RemoveAll(dir)
	}
	return d, cleanup
}

func newBenchPersistentGenUUID(b *testing.B) (*PersistentGenUUID, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "mono_uuid_bench-*")
	if err != nil {
		b.Fatalf("MkdirTemp: %v", err)
	}
	filename := filepath.Join(dir, "bench.wal")

	d, err := OpenGenUUID(1, filename)
	if err != nil {
		b.Fatalf("OpenGenUUID: %v", err)
	}

	cleanup := func() {
		_ = d.Close()
		_ = os.RemoveAll(dir)
	}
	return d, cleanup
}

func BenchmarkPersistentGenNext(b *testing.B) {
	d, cleanup := newBenchPersistentGen(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.Next(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkPersistentGenUUIDNext(b *testing.B) {
	d, cleanup := newBenchPersistentGenUUID(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.Next(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkPersistentGenUUIDNextParallel(b *testing.B) {
	d, cleanup := newBenchPersistentGenUUID(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := d.Next(); err != nil {
				b.Logf("Parallel Next(): %v", err)
				b.Fail()
			}
		}
	})
	b.StopTimer()
}
