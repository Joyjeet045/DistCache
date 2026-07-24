package persistence

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"

	"distcache/internal/cache"
)

const headerBytes = 1 + 8 + 4 + 4 + 8

func encodeRecord(ev cache.Event) []byte {
	payloadLen := headerBytes + len(ev.Key) + len(ev.Value)
	buf := make([]byte, 4+payloadLen)
	binary.BigEndian.PutUint32(buf[0:], uint32(payloadLen))
	p := buf[4:]
	p[0] = byte(ev.Op)
	binary.BigEndian.PutUint64(p[1:], ev.Seq)
	binary.BigEndian.PutUint32(p[9:], uint32(len(ev.Key)))
	binary.BigEndian.PutUint32(p[13:], uint32(len(ev.Value)))
	binary.BigEndian.PutUint64(p[17:], uint64(ev.ExpireAt))
	off := 25
	off += copy(p[off:], ev.Key)
	copy(p[off:], ev.Value)
	return buf
}

var errTornRecord = errors.New("persistence: torn trailing record")

func decodeRecord(r *bufio.Reader) (cache.Event, error) {
	var lenBuf [4]byte
	n, err := io.ReadFull(r, lenBuf[:])
	if err == io.EOF && n == 0 {
		return cache.Event{}, io.EOF
	}
	if err != nil {
		return cache.Event{}, errTornRecord
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen < headerBytes {
		return cache.Event{}, errTornRecord
	}
	p := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, p); err != nil {
		return cache.Event{}, errTornRecord
	}
	ev := cache.Event{
		Op:       cache.Op(p[0]),
		Seq:      binary.BigEndian.Uint64(p[1:]),
		ExpireAt: int64(binary.BigEndian.Uint64(p[17:])),
	}
	keyLen := binary.BigEndian.Uint32(p[9:])
	valLen := binary.BigEndian.Uint32(p[13:])
	if uint32(len(p)) != uint32(headerBytes)+keyLen+valLen {
		return cache.Event{}, errTornRecord
	}
	off := 25
	ev.Key = string(p[off : off+int(keyLen)])
	off += int(keyLen)
	if valLen > 0 {
		ev.Value = append([]byte(nil), p[off:off+int(valLen)]...)
	}
	return ev, nil
}
