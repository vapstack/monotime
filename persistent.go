package monotime

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// PersistentGen provides a persistence wrapper around a Gen by storing its state to a write-ahead log.
type PersistentGen struct {
	gen *Gen
	wal *wal
}

// OpenGen opens or creates a log file using the provided filename
// and returns PersistentGen with its state restored from the last entry in the log.
func OpenGen(filename string) (*PersistentGen, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return nil, err
	}
	w, last, err := walOpen(filename, 8, 2_000_000)
	if err != nil {
		return nil, fmt.Errorf("error opening WAL: %w", err)
	}
	var g *Gen
	if len(last) == 0 {
		g = NewGen(0)
	} else {
		g = NewGen(int64(binary.BigEndian.Uint64(last)))
	}
	d := &PersistentGen{
		gen: g,
		wal: w,
	}
	return d, nil
}

// Next generates a new timestamp and synchronously persists it to disk before returning.
// The generation logic is identical to Gen.Next.
// This method returns an error only if the write fails.
func (d *PersistentGen) Next() (int64, error) {
	v := d.gen.Next()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return v, d.wal.store(b[:])
}

// Close flushes and closes the underlying WAL file.
func (d *PersistentGen) Close() error {
	return d.wal.close()
}

/**/

// PersistentGenUUID provides a persistence wrapper around a GenUUID by storing its state to a write-ahead log.
type PersistentGenUUID struct {
	gen *GenUUID
	wal *wal
}

// OpenGenUUID opens or creates a log file using the provided filename
// and returns PersistentGenUUID with its state restored from the last entry in the log.
// It will return an error if the provided nodeID does not match the node ID stored in the log.
func OpenGenUUID(nodeID int, filename string) (*PersistentGenUUID, error) {
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return nil, err
	}
	w, last, err := walOpen(filename, 16, 1_000_000)
	if err != nil {
		return nil, fmt.Errorf("error opening WAL: %w", err)
	}
	var g *GenUUID
	if len(last) == 0 {
		g, err = NewGenUUID(nodeID, ZeroUUID)
	} else {
		g, err = NewGenUUID(nodeID, UUID(last))
	}
	if err != nil {
		return nil, err
	}
	d := &PersistentGenUUID{
		gen: g,
		wal: w,
	}
	return d, nil
}

// Next generates a new UUID and synchronously saves it to disk before returning.
// The generation logic is identical to GenUUID.Next.
// This method returns an error only if the write fails.
func (d *PersistentGenUUID) Next() (UUID, error) {
	v := d.gen.Next()
	return v, d.wal.store(v[:])
}

// Close flushes and closes the underlying WAL file.
func (d *PersistentGenUUID) Close() error {
	return d.wal.close()
}

/**/

type wal struct {
	mu    sync.Mutex
	file  *os.File
	name  string
	max   int
	count int
}

func walOpen(filename string, entrySize int, maxEntries int) (*wal, []byte, error) {

	var lastEntry []byte

	if f, ferr := os.Open(filename); ferr != nil {
		if !errors.Is(ferr, fs.ErrNotExist) {
			return nil, nil, ferr
		}

	} else {
		err := func() error {
			defer closeFile(f)

			s, err := f.Stat()
			if err != nil {
				return fmt.Errorf("WAL stat: %w", err)
			}

			fileSize := s.Size()
			if fileSize >= int64(entrySize) {

				lastFull := (fileSize / int64(entrySize)) * int64(entrySize)

				if lastFull > 0 {

					lastEntry = make([]byte, entrySize)

					_, err = f.ReadAt(lastEntry, lastFull-int64(entrySize))
					if err != nil && err != io.EOF {
						return fmt.Errorf("error reading WAL: %w", err)
					}
				}
			}
			return nil
		}()
		if err != nil {
			return nil, nil, err
		}
	}

	f, _, err := replaceFile(filename, lastEntry)
	if err != nil {
		return nil, nil, err
	}

	w := &wal{
		file: f,
		name: filename,
		max:  maxEntries,
	}

	return w, lastEntry, nil
}

func replaceFile(filename string, data []byte) (*os.File, bool, error) {

	tempname := filename + ".monotemp"

	t, err := os.OpenFile(tempname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, true, fmt.Errorf("error creating temp file: %w", err)
	}

	if data != nil {
		if _, err = t.Write(data); err != nil {
			closeFile(t)
			removeFile(tempname)
			return nil, true, fmt.Errorf("write error: %v", err)
		}
	}

	if err = t.Sync(); err != nil {
		closeFile(t)
		removeFile(tempname)
		return nil, true, fmt.Errorf("sync error (on temp): %v", err)
	}

	if err = t.Close(); err != nil {
		removeFile(tempname)
		return nil, true, fmt.Errorf("close error: %v", err)
	}

	if err = os.Rename(tempname, filename); err != nil {
		return nil, true, fmt.Errorf("rename error: %v", err)
	}

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("error opening newly created file: %w", err)
	}

	return f, true, nil
}

func (w *wal) rotate(data []byte) (bool, error) {

	f, canContinue, err := replaceFile(w.name, data)
	if err != nil {
		return canContinue, fmt.Errorf("error replacing WAL file: %w", err)
	}

	defer closeFile(w.file)

	w.file = f
	w.count = 0

	return true, nil
}

func (w *wal) store(data []byte) error {

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL is closed")
	}

	if w.count >= w.max {

		if canContinue, err := w.rotate(data); err != nil {

			if canContinue {
				log.Println("monotime: error replacing WAL file:", err)

			} else {
				closeFile(w.file)
				w.file = nil

				return fmt.Errorf("error rotating WAL: %w", err)
			}

		} else {
			return nil
		}
	}

	_, err := w.file.Write(data)
	if err != nil {
		return err
	}

	w.count++

	if err = w.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *wal) close() error {

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		serr := w.file.Sync()
		cerr := w.file.Close()
		w.file = nil
		if cerr != nil {
			return cerr
		} else {
			return serr
		}
	}

	return nil
}

func closeFile(f *os.File) {
	if err := f.Close(); err != nil {
		log.Println("monotime: error closing file:", err)
	}
}

func removeFile(name string) {
	if err := os.Remove(name); err != nil {
		log.Println("monotime: error removing file:", err)
	}
}
