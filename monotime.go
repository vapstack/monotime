// Package monotime provides fast, lock-free, and strictly monotonic time
// and UUID v7 generators.
//
// These generators are guaranteed to never go backward, even in the event of
// system clock adjustments or NTP jumps, ensuring strict monotonicity and uniqueness.
package monotime

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"
)

// Monotime provides a simple, lock-free, and strictly monotonic time source
// that generates nanosecond timestamps.
type Monotime struct {
	last atomic.Int64
}

// New creates a new monotonic timestamp generator.
// The generator guarantees strictly monotonic, ever-increasing values,
// even if the system clock moves backwards or stops.
// If lastKnown is greater than the current system time, the generator continues from it.
func New(lastKnown int64) *Monotime {
	g := new(Monotime)
	g.last.Store(max(lastKnown, time.Now().UnixNano()))
	return g
}

// Next returns the next strictly monotonic nanosecond timestamp.
// The returned value will always be greater than any previously returned
// value from this generator. Next never blocks, even if the system
// clock moves backwards or stops; it atomically increments the last timestamp by 1.
// Next is lock-free and safe to use from multiple goroutines.
func (g *Monotime) Next() int64 {
	for {
		old := g.last.Load()
		now := time.Now().UnixNano()
		var next int64
		if now > old {
			next = now
		} else {
			next = old + 1
		}
		if g.last.CompareAndSwap(old, next) {
			return next
		}
	}
}

/**/

// 62-bit rand_b layout (using 60 bits out of the 62 available):
// [ 16 bits: Prefix | 24 bits: NodeID | 20 bits: NanoRemainder ]
// The remaining bits (and the 12 bits of 'rand_a') will be 0,
// strengthening magic signature.
const (
	monoPrefix uint16 = 0xE355 // magic prefix

	nodeMask uint32 = (1 << 24) - 1 // 0xFFFFFF (3 bytes, ~16M)
	nanoMask uint32 = (1 << 20) - 1 // 0xFFFFF  (20 bits, ~1.04M)
)

// Bit shifts for packing/unpacking the 62-bit rand_b (uint64):
const (
	nanoShift   = 0
	nodeShift   = 20
	prefixShift = 20 + 24 // 44
)

// UUID represents a 16-byte UUID v7 value following RFC 9562
// and supporting the custom "monotime" extension format.
//
// A monotime UUID embeds:
//   - a 48-bit timestamp in milliseconds,
//   - a 12-bit zeroed rand_a field (used as signature),
//   - a 62-bit custom rand_b field containing a fixed prefix, node ID (24 bits) and a 20-bit nanosecond remainder.
//
// This layout allows round-tripping back to the original time with
// full nanosecond precision and provides detection of monotime-encoded UUIDs.
type UUID [16]byte

// Parse extracts time, node ID, and a flag indicating whether the UUID
// conforms to the monotime UUID format.
//
// If the UUID is not a valid monotime UUID (incorrect version, variant,
// signature bits, or prefix), flag will be false.
// Otherwise, it returns the full timestamp reconstructed with nanosecond
// precision and the encoded node ID.
func (u UUID) Parse() (time.Time, int, bool) {
	node, nano, ok := u.parse()
	if !ok {
		return time.Time{}, 0, false
	}
	return u.time(nano), node, true
}

func (u UUID) parse() (node int, nano uint32, ok bool) {

	// v7 and variant (10xx)
	if (u[6]&0xF0) != 0x70 || (u[8]&0xC0) != 0x80 {
		return 0, 0, false
	}

	// rand_a (12 bits) must be zero (as part of signature)
	if (u[6]&0x0F) != 0x00 || u[7] != 0x00 {
		return 0, 0, false
	}

	// extract the 62-bit rand_b as uint64
	var payloadBytes [8]byte
	payloadBytes[0] = u[8] & 0x3F // clear the variant bits `10`
	copy(payloadBytes[1:8], u[9:16])
	payload := binary.BigEndian.Uint64(payloadBytes[:])

	// check magic prefix
	prefix := uint16(payload >> prefixShift)
	if prefix != monoPrefix {
		return 0, 0, false
	}

	node = int(uint32(payload>>nodeShift) & nodeMask)
	nano = uint32(payload>>nanoShift) & nanoMask

	return node, nano, true
}

func (u UUID) time(nano uint32) time.Time {

	// reconstruct the time
	ms := (uint64(u[0]) << 40) |
		(uint64(u[1]) << 32) |
		(uint64(u[2]) << 24) |
		(uint64(u[3]) << 16) |
		(uint64(u[4]) << 8) |
		(uint64(u[5]))

	// build nanoseconds
	sec := int64(ms / 1000)
	nsec := int64(ms%1000)*1_000_000 + int64(nano)

	return time.Unix(sec, nsec)
}

// Time returns the timestamp embedded in the UUID.
// If the UUID is not a valid monotime UUID, it returns zero time.
// This method preserves full nanosecond precision reconstructed from
// the custom rand_b field.
func (u UUID) Time() time.Time {
	_, nano, ok := u.parse()
	if !ok {
		return time.Time{}
	}
	return u.time(nano)
}

// NodeID returns the node ID embedded in the UUID.
// It returns 0 if the UUID is not a valid monotime UUID.
func (u UUID) NodeID() int {
	node, _, _ := u.parse()
	return node
}

// String implements fmt.Stringer and returns the canonical
// string representation of the UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func (u UUID) String() string {
	b := u.bytestr()
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (u UUID) bytestr() []byte {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return buf[:]
}

// MarshalText implements encoding.TextMarshaler.
// It returns the canonical 36-byte string representation of the UUID.
func (u UUID) MarshalText() ([]byte, error) {
	return u.bytestr(), nil
}

// UnmarshalText implements encoding.TextUnmarshaler and parses a UUID
// from its canonical 36-character string form.
func (u *UUID) UnmarshalText(text []byte) error {
	if len(text) != 36 {
		return fmt.Errorf("invalid UUID string length: want %d, got %d", 36, len(text))
	}
	if text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return fmt.Errorf("invalid UUID string format")
	}

	var buf [32]byte
	copy(buf[0:8], text[0:8])
	copy(buf[8:12], text[9:13])
	copy(buf[12:16], text[14:18])
	copy(buf[16:20], text[19:23])
	copy(buf[20:32], text[24:36])

	if _, err := hex.Decode(u[:], buf[:]); err != nil {
		return err
	}
	if _, _, ok := u.parse(); !ok {
		return fmt.Errorf("the provided UUID is not a monotime UUID")
	}
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler and returns a copy of the raw 16-byte UUID value.
func (u UUID) MarshalBinary() ([]byte, error) {
	return bytes.Clone(u[:]), nil // can we just return u[:] without copy?
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler and loads the UUID from a 16-byte binary slice.
func (u *UUID) UnmarshalBinary(data []byte) error {
	if len(data) != 16 {
		return fmt.Errorf("invalid UUID binary length: %d", len(data))
	}
	copy(u[:], data)

	if _, _, ok := u.parse(); !ok {
		// this breaks standard UUID compatibility,
		// but it's better than storing invalid monotime UUID and then passing it to NewMonoUUID
		return fmt.Errorf("the provided UUID is not a monotime UUID")
	}

	return nil
}

// Value implements sql.Valuer and returns the UUID as a string suitable for storing in SQL databases.
func (u UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

// Scan implements sql.Scanner and loads a UUID from either binary or text SQL column types.
// It accepts:
//   - string (text form),
//   - []byte (binary form or text form),
//
// and returns an error for unsupported types.
func (u *UUID) Scan(src any) error {
	if src == nil {
		*u = UUID{}
		return nil
	}
	switch s := src.(type) {
	case string:
		return u.UnmarshalText(unsafe.Slice(unsafe.StringData(s), len(s)))
	case []byte:
		if len(s) == 16 {
			return u.UnmarshalBinary(s)
		}
		return u.UnmarshalText(s) // attempt as text
	default:
		return fmt.Errorf("cannot scan type %T into monotime UUID", src)
	}
}

// MonoUUID generates strictly monotonic, K-sortable UUID v7 identifiers.
//
// The generator is lock-free and guarantees that each generated UUID is
// strictly greater than the previous one generated by the same node ID.
// It is resilient to system clock rollbacks or standstills (e.g., NTP adjustments),
// in which case it will increment the nanosecond counter logically to ensure
// monotonicity and uniqueness.
//
// The generated UUIDs contain a custom magic prefix and zeroed rand_a field
// to distinguish them from standard UUID v7 implementations.
type MonoUUID struct {
	lastNano atomic.Int64

	// static part of rand_b is pre-calculated for maximum performance
	staticPayload uint64
}

// ZeroUUID represents an empty (zero) monotime UUID value which indicates that
// no prior UUID is known when initializing a generator.
var ZeroUUID UUID

// NewMonoUUID creates a new monotime UUID generator bound to the given node ID.
//
// If lastKnown is ZeroUUID, the generator starts from the current system
// time. Otherwise, lastKnown must be a valid monotime UUID previously
// produced by the same node; its timestamp is used to ensure monotonicity.
//
// Returns an error if:
//   - nodeID is zero or outside the 24-bit range,
//   - lastKnown is not a valid monotime UUID,
//   - lastKnown was generated by a different node.
func NewMonoUUID(nodeID int, lastKnown UUID) (*MonoUUID, error) {

	if nodeID <= 0 || int(uint32(nodeID)&nodeMask) != nodeID {
		return nil, fmt.Errorf("node id must be in range: 1..%v (24 bits)", 1<<24-1)
	}

	// pre-calculate static rand_b
	prefixPayload := uint64(monoPrefix) << prefixShift
	nodePayload := uint64(nodeID) << nodeShift
	staticPayload := prefixPayload | nodePayload

	var lastNano int64

	if lastKnown != ZeroUUID {

		t, parsedNodeID, ok := lastKnown.Parse()
		if !ok {
			return nil, fmt.Errorf("last known UUID is not a monotime UUID")
		}
		if parsedNodeID != nodeID {
			return nil, fmt.Errorf("node ID mismatch: provided: %v, last known: %v", nodeID, parsedNodeID)
		}
		lastNano = t.UnixNano()
	}

	g := &MonoUUID{
		staticPayload: staticPayload,
	}

	g.lastNano.Store(max(lastNano, time.Now().UnixNano()))
	return g, nil
}

// Next generates the next strictly monotonic monotime UUID.
//
// The returned UUID is always greater than any previously generated UUID,
// even if the system clock jumps backwards or stops.
//
// Next is lock-free and safe to use from multiple goroutines.
func (g *MonoUUID) Next() UUID {
	var nextNano int64
	for {
		old := g.lastNano.Load()
		now := time.Now().UnixNano()
		if now > old {
			nextNano = now
		} else {
			// time is standing still, rolled back, or we're in "logical" mode
			nextNano = old + 1
		}
		if g.lastNano.CompareAndSwap(old, nextNano) {
			break
		}
	}

	// decompose into UUID v7 components

	ms := uint64(nextNano / 1_000_000)

	nanoRemainder := uint32(nextNano) & nanoMask // ~

	// (nextNano % 1_000_000) is always < 1M and fits in 20 bits, should we use it?
	// nanoRemainder := uint32(nextNano % 1_000_000)

	var id [16]byte

	// pack the UUID v7 byte by byte

	// [0:6] (48 bits): Time (milliseconds)
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)

	// version (0111) + rand_a (top 4 bits)
	// rand_a (12 bits) will be 0 for signature
	id[6] = 0x70 // 0111_0000

	// rand_a (bottom 8 bits) already zero
	// id[7] = 0x00

	// 62-bit rand_b (staticPayload already contains prefix + node)
	payload := g.staticPayload | (uint64(nanoRemainder) << nanoShift)

	var payloadBytes [8]byte
	binary.BigEndian.PutUint64(payloadBytes[:], payload)

	// copy the bytes, applying the variant mask `10` to the first
	id[8] = (payloadBytes[0] & 0x3F) | 0x80 // 00111111 + 10000000
	copy(id[9:16], payloadBytes[1:8])

	return id
}
