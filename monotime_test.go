package monotime

import (
	"bytes"
	"encoding/hex"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func TestMonotimeMonotonicity(t *testing.T) {
	m := New(0)
	prev := m.Next()
	for i := 0; i < 1_000_000; i++ {
		n := m.Next()
		if n <= prev {
			t.Fatalf("timestamps not strictly monotonic: %d <= %d", n, prev)
		}
		prev = n
	}
}

func TestMonotimeConcurrency(t *testing.T) {
	m := New(0)

	const goroutines = 8
	const perG = 5000

	results := make(chan int64, goroutines*perG)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				results <- m.Next()
			}
		}()
	}

	wg.Wait()
	close(results)

	prev := int64(-1)
	for v := range results {
		if prev != -1 && v == prev {
			t.Fatalf("duplicate timestamp detected: %d", v)
		}
		prev = v
	}
}

/**/

func TestMonoUUIDMonotonicity(t *testing.T) {
	g, err := NewMonoUUID(123, ZeroUUID)
	if err != nil {
		t.Fatal(err)
	}
	prev := g.Next()
	for i := 0; i < 1000000; i++ {
		n := g.Next()
		if bytes.Compare(n[:], prev[:]) <= 0 {
			t.Fatalf("UUIDs not strictly monotonic: %x <= %x", n, prev)
		}
		prev = n
	}
}

func TestMonoUUIDConcurrency(t *testing.T) {
	m, err := NewMonoUUID(5, ZeroUUID)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const perG = 5000

	results := make(chan UUID, goroutines*perG)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				results <- m.Next()
			}
		}()
	}

	wg.Wait()
	close(results)

	prev := ZeroUUID
	for v := range results {
		if prev != ZeroUUID && v == prev {
			t.Fatalf("duplicate timestamp detected: %d", v)
		}
		prev = v
	}
}

func TestMonoUUIDNodeID(t *testing.T) {
	node := 987654
	g, err := NewMonoUUID(node, ZeroUUID)
	if err != nil {
		t.Fatal(err)
	}
	id := g.Next()

	n := id.NodeID()
	if n != node {
		t.Fatalf("unexpected node id: got %d, want %d", n, node)
	}
}

func TestMonoUUIDRollback(t *testing.T) {
	g, err := NewMonoUUID(42, ZeroUUID)
	if err != nil {
		t.Fatal(err)
	}
	first := g.Next()

	// simulate clock rollback by mocking lastNano
	g.lastNano.Add(10)
	second := g.Next()

	if bytes.Compare(second[:], first[:]) <= 0 {
		t.Fatalf("UUID after rollback is not strictly greater")
	}
}

func TestUUIDParseRoundTrip(t *testing.T) {
	g, _ := NewMonoUUID(1001, ZeroUUID)

	for i := 0; i < 10000; i++ {
		id := g.Next()

		tm, node, ok := id.Parse()
		if !ok {
			t.Fatalf("Parse returned ok=false for valid UUID: %x", id)
		}
		if node != 1001 {
			t.Fatalf("wrong node id: got %d, want %d", node, 1001)
		}

		// reconstruct unixnano
		if tm.UnixNano() != id.Time().UnixNano() {
			t.Fatalf("time round-trip mismatch: %d != %d", tm.UnixNano(), id.Time().UnixNano())
		}
	}
}

func TestUUIDInvalidVersion(t *testing.T) {
	var u UUID
	copy(u[:], make([]byte, 16))

	// wrong version
	u[6] = 0x40 // not 0x70
	_, _, ok := u.Parse()
	if ok {
		t.Fatal("Parse should fail on invalid version")
	}
}

func TestUUIDInvalidVariant(t *testing.T) {
	id := ZeroUUID
	id[6] = 0x70
	id[8] = 0x00 // invalid variant (should be 0b10xxxxxx)
	_, _, ok := id.Parse()
	if ok {
		t.Fatal("Parse should fail on invalid variant")
	}
}

func TestUUIDv7InvalidPrefix(t *testing.T) {
	g, _ := NewMonoUUID(1, ZeroUUID)
	id := g.Next()

	// break prefix
	id[8] ^= 0x02

	_, _, ok := id.Parse()
	if ok {
		t.Fatal("Parse should fail on incorrect prefix")
	}
}

func TestBinaryMarshalUnmarshal(t *testing.T) {
	g, _ := NewMonoUUID(55, ZeroUUID)

	id := g.Next()

	bin, err := id.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// mutate returned buffer — must not mutate original UUID
	bin[9] ^= 0xFF

	var out UUID
	if err = out.UnmarshalBinary(bin); err == nil {
		t.Fatal("expected error from corrupted binary UUID")
	}
}

func TestUnmarshalBinaryRejectsInvalid(t *testing.T) {
	var u UUID
	err := u.UnmarshalBinary(make([]byte, 16))
	if err == nil {
		t.Fatal("expected error for invalid monotime UUID")
	}
}

func TestUUIDScanRejectsInvalid(t *testing.T) {
	var u UUID
	err := u.Scan([]byte("not-a-valid-uuid"))
	if err == nil {
		t.Fatal("expected Scan to fail for invalid UUID text")
	}
}

func TestUUIDStringFormat(t *testing.T) {
	g, _ := NewMonoUUID(1, ZeroUUID)
	id := g.Next()

	s := id.String()
	if len(s) != 36 {
		t.Fatalf("UUID string length must be 36, got %d", len(s))
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		t.Fatalf("invalid UUID format: %s", s)
	}
}

func TestUnmarshalTextRejectsInvalidLength(t *testing.T) {
	var u UUID
	err := u.UnmarshalText([]byte("too-short"))
	if err == nil {
		t.Fatal("expected error for short UUID")
	}
}

func TestUUIDUnmarshalErrors(t *testing.T) {
	var u UUID

	testCasesText := map[string]string{
		"bad format":   "00000000-0000-7000-8000-00000000000G", // G is bad hex
		"bad hyphens":  "00000000_0000-7000-8000-000000000000",
		"not monotime": "018f3a38-641a-7c30-817f-682f7e0b704e", // valid v7, bad rand_a
		"wrong prefix": "018f3a38-641a-7000-8000-000000000000", // valid v7, 0 rand_a, bad prefix
	}
	for name, tc := range testCasesText {
		t.Run(name, func(t *testing.T) {
			if err := u.UnmarshalText([]byte(tc)); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}

	if err := u.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
		t.Error("UnmarshalBinary: expected error for wrong length, got nil")
	}

	badBin := mustDecodeHex(t, "018f3a38641a7c30817f682f7e0b704e")
	if err := u.UnmarshalBinary(badBin); err == nil {
		t.Error("UnmarshalBinary: expected error for non-monotime UUID, got nil")
	}

	if err := u.Scan(12345); err == nil {
		t.Error("Scan: expected error for invalid type (int), got nil")
	}
}

var uuidRegex = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestUUIDTextRoundtrip(t *testing.T) {
	g, err := NewMonoUUID(1, ZeroUUID)
	if err != nil {
		t.Fatalf("NewMonoUUID failed: %v", err)
	}
	u1 := g.Next()

	s := u1.String()
	if !uuidRegex.MatchString(s) {
		t.Fatalf("String() format is invalid: %s", s)
	}

	text, err := u1.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText() failed: %v", err)
	}
	if string(text) != s {
		t.Fatalf("MarshalText() mismatch: expected %s, got %s", s, string(text))
	}

	var u2 UUID
	if err = u2.UnmarshalText(text); err != nil {
		t.Fatalf("UnmarshalText() failed: %v", err)
	}
	if u1 != u2 {
		t.Fatalf("Text roundtrip failed: original: %s, result: %s", u1.String(), u2.String())
	}
}

func TestUUIDBinaryRoundtrip(t *testing.T) {
	g, err := NewMonoUUID(1, ZeroUUID)
	if err != nil {
		t.Fatalf("NewMonoUUID failed: %v", err)
	}
	u1 := g.Next()

	bin, err := u1.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() failed: %v", err)
	}
	if len(bin) != 16 {
		t.Fatalf("MarshalBinary() length: expected 16, got %d", len(bin))
	}
	if !bytes.Equal(bin, u1[:]) {
		t.Error("MarshalBinary() did not return expected bytes")
	}

	bin[0] = 0xFF
	if u1[0] == 0xFF {
		t.Fatal("MarshalBinary() did not return a copy!")
	}

	var u2 UUID
	bin[0] = u1[0]
	if err = u2.UnmarshalBinary(bin); err != nil {
		t.Fatalf("UnmarshalBinary() failed: %v", err)
	}

	if u1 != u2 {
		t.Fatalf("Binary roundtrip failed: original: %v, result: %v", u1, u2)
	}
}

func TestUUIDSQLRoundtrip(t *testing.T) {
	g, err := NewMonoUUID(99, ZeroUUID)
	if err != nil {
		t.Fatalf("NewMonoUUID failed: %v", err)
	}
	u1 := g.Next()

	val, err := u1.Value()
	if err != nil {
		t.Fatalf("Value() failed: %v", err)
	}
	s, ok := val.(string)
	if !ok {
		t.Fatalf("Value() did not return a string, got %T", val)
	}
	if s != u1.String() {
		t.Fatal("Value() returned wrong string")
	}

	var u2 UUID
	if err = u2.Scan(s); err != nil {
		t.Fatalf("Scan(string) failed: %v", err)
	}
	if u1 != u2 {
		t.Fatal("Scan(string) roundtrip failed")
	}

	var u3 UUID
	if err = u3.Scan([]byte(s)); err != nil {
		t.Fatalf("Scan([]byte text) failed: %v", err)
	}
	if u1 != u3 {
		t.Fatal("Scan([]byte text) roundtrip failed")
	}

	bin, _ := u1.MarshalBinary()
	var u4 UUID
	if err = u4.Scan(bin); err != nil {
		t.Fatalf("Scan([]byte binary) failed: %v", err)
	}
	if u1 != u4 {
		t.Fatal("Scan([]byte binary) roundtrip failed")
	}

	var u5 UUID
	if err = u5.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) failed: %v", err)
	}
	if u5 != ZeroUUID {
		t.Fatal("Scan(nil) did not produce ZeroUUID")
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	s = strings.Replace(s, "-", "", -1)
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

/**/

var (
	benchGen     = New(0)
	benchUUIDGen = mustNewBenchGen()
	benchUUID    = benchUUIDGen.Next()
	benchUUIDStr = benchUUID.String()
)

func mustNewBenchGen() *MonoUUID {
	g, err := NewMonoUUID(1, ZeroUUID)
	if err != nil {
		panic(err)
	}
	return g
}

func BenchmarkMonotime_Next(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchGen.Next()
	}
}

func BenchmarkMonotime_NextParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchGen.Next()
		}
	})
}

func BenchmarkMonoUUID_Next(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchUUIDGen.Next()
	}
}

func BenchmarkMonoUUID_NextParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchUUIDGen.Next()
		}
	})
}

func BenchmarkUUID_String(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = benchUUID.String()
	}
}

func BenchmarkUUID_MarshalText(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = benchUUID.MarshalText()
	}
}

func BenchmarkUUID_Parse(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = benchUUID.Parse()
	}
}

func BenchmarkUUID_UnmarshalText(b *testing.B) {
	var u UUID
	v := []byte(benchUUIDStr)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = u.UnmarshalText(v)
	}
}

func BenchmarkUUID_UnmarshalBinary(b *testing.B) {
	var u UUID
	v, _ := benchUUID.MarshalBinary()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = u.UnmarshalBinary(v)
	}
}
