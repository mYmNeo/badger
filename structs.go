package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/y"
)

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

func (p valuePointer) Less(o valuePointer) bool {
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p valuePointer) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

const vptrSize = int(unsafe.Sizeof(valuePointer{}))

// Encode encodes Pointer into byte buffer.
func (p valuePointer) Encode() []byte {
	var b [vptrSize]byte
	// Copy over the content from p to b.
	*(*valuePointer)(unsafe.Pointer(&b[0])) = p
	return b[:]
}

func (p *valuePointer) Decode(b []byte) {
	// Copy over data from b into p. Using *p=unsafe.pointer(...) leads to
	// pointer alignment issues. See https://github.com/dgraph-io/badger/issues/1096
	// and comment https://github.com/dgraph-io/badger/pull/1097#pullrequestreview-307361714
	copy((*[vptrSize]byte)(unsafe.Pointer(p))[:], b[:vptrSize])
}

// header is used in value log as a header before Entry.
type header struct {
	klen      uint32
	vlen      uint32
	expiresAt uint64
	meta      byte
	userMeta  byte
}

const (
	headerBufSize = 18
)

func (h header) Encode(out []byte) {
	y.AssertTrue(len(out) >= headerBufSize)
	binary.BigEndian.PutUint32(out[0:4], h.klen)
	binary.BigEndian.PutUint32(out[4:8], h.vlen)
	binary.BigEndian.PutUint64(out[8:16], h.expiresAt)
	out[16] = h.meta
	out[17] = h.userMeta
}

// Decodes h from buf.
func (h *header) Decode(buf []byte) {
	h.klen = binary.BigEndian.Uint32(buf[0:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	h.expiresAt = binary.BigEndian.Uint64(buf[8:16])
	h.meta = buf[16]
	h.userMeta = buf[17]
}

// Entry provides Key, Value, UserMeta and ExpiresAt. This struct can be used by
// the user to set data.
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64 // time.Unix
	offset    uint32
	UserMeta  byte
	meta      byte
	// Fields maintained internally.
	skipVlog bool
}

func (e *Entry) estimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}
	return len(e.Key) + vptrSize + 2 // 12 for ValuePointer, 2 for metas.
}

var headerPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, headerBufSize)
	},
}

var crcPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, crc32.Size)
	},
}

// Encodes e to buf. Returns number of bytes written.
func encodeEntry(e *Entry, buf *bytes.Buffer) (int, error) {
	h := header{
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
		meta:      e.meta,
		userMeta:  e.UserMeta,
	}

	headerEnc := headerPool.Get().([]byte)
	defer headerPool.Put(headerEnc)

	h.Encode(headerEnc)

	hash := crc32.New(y.CastagnoliCrcTable)

	buf.Write(headerEnc)
	if _, err := hash.Write(headerEnc); err != nil {
		return 0, err
	}

	buf.Write(e.Key)
	if _, err := hash.Write(e.Key); err != nil {
		return 0, err
	}

	buf.Write(e.Value)
	if _, err := hash.Write(e.Value); err != nil {
		return 0, err
	}

	crcBuf := crcPool.Get().([]byte)
	defer crcPool.Put(crcBuf)

	binary.BigEndian.PutUint32(crcBuf, hash.Sum32())
	buf.Write(crcBuf)

	return len(headerEnc) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d UserMeta: %d Offset: %d len(val)=%d",
		prefix, e.Key, e.meta, e.UserMeta, e.offset, len(e.Value))
}

// NewEntry creates a new entry with key and value passed in args. This newly created entry can be
// set in a transaction by calling txn.SetEntry(). All other properties of Entry can be set by
// calling WithMeta, WithDiscard, WithTTL methods on it.
// This function uses key and value reference, hence users must
// not modify key and value until the end of transaction.
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// WithMeta adds meta data to Entry e. This byte is stored alongside the key
// and can be used as an aid to interpret the value or store other contextual
// bits corresponding to the key-value pair of entry.
func (e *Entry) WithMeta(meta byte) *Entry {
	e.UserMeta = meta
	return e
}

// WithDiscard adds a marker to Entry e. This means all the previous versions of the key (of the
// Entry) will be eligible for garbage collection.
// This method is only useful if you have set a higher limit for options.NumVersionsToKeep. The
// default setting is 1, in which case, this function doesn't add any more benefit. If however, you
// have a higher setting for NumVersionsToKeep (in Dgraph, we set it to infinity), you can use this
// method to indicate that all the older versions can be discarded and removed during compactions.
func (e *Entry) WithDiscard() *Entry {
	e.meta = bitDiscardEarlierVersions
	return e
}

// WithTTL adds time to live duration to Entry e. Entry stored with a TTL would automatically expire
// after the time has elapsed, and will be eligible for garbage collection.
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// withMergeBit sets merge bit in entry's metadata. This
// function is called by MergeOperator's Add method.
func (e *Entry) withMergeBit() *Entry {
	e.meta = bitMergeEntry
	return e
}
