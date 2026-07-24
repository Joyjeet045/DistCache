package replication

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"distcache/internal/cache"
)

const replMagic = "DCREPL1\n"

const maxFrame = 512 << 20

const (
	frameEvent  byte = 1
	frameSynced byte = 2
	frameAck    byte = 3
)

const eventHeader = 1 + 8 + 4 + 4 + 8

var (
	errFrameTooLarge = errors.New("replication: frame too large")
	errBadEvent      = errors.New("replication: malformed event")
)

func writeFrame(w *bufio.Writer, typ byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > maxFrame {
		return 0, nil, errFrameTooLarge
	}
	if n == 0 {
		return hdr[0], nil, nil
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(r, p); err != nil {
		return 0, nil, err
	}
	return hdr[0], p, nil
}

func encodeEvent(ev cache.Event) []byte {
	buf := make([]byte, eventHeader+len(ev.Key)+len(ev.Value))
	buf[0] = byte(ev.Op)
	binary.BigEndian.PutUint64(buf[1:], ev.Seq)
	binary.BigEndian.PutUint32(buf[9:], uint32(len(ev.Key)))
	binary.BigEndian.PutUint32(buf[13:], uint32(len(ev.Value)))
	binary.BigEndian.PutUint64(buf[17:], uint64(ev.ExpireAt))
	off := 25
	off += copy(buf[off:], ev.Key)
	copy(buf[off:], ev.Value)
	return buf
}

func decodeEvent(p []byte) (cache.Event, error) {
	if len(p) < eventHeader {
		return cache.Event{}, errBadEvent
	}
	keyLen := binary.BigEndian.Uint32(p[9:])
	valLen := binary.BigEndian.Uint32(p[13:])
	if uint32(len(p)) != uint32(eventHeader)+keyLen+valLen {
		return cache.Event{}, errBadEvent
	}
	ev := cache.Event{
		Op:       cache.Op(p[0]),
		Seq:      binary.BigEndian.Uint64(p[1:]),
		ExpireAt: int64(binary.BigEndian.Uint64(p[17:])),
	}
	off := 25
	ev.Key = string(p[off : off+int(keyLen)])
	off += int(keyLen)
	if valLen > 0 {
		ev.Value = append([]byte(nil), p[off:off+int(valLen)]...)
	}
	return ev, nil
}

func u64Bytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func readU64(p []byte) (uint64, bool) {
	if len(p) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(p), true
}
