package persistence

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"distcache/internal/cache"
)

var snapMagic = [8]byte{'D', 'C', 'S', 'N', 'A', 'P', '1', '\n'}

var ErrBadSnapshot = errors.New("persistence: invalid snapshot header")

func WriteSnapshot(path string, c *cache.Cache) (startSeq uint64, err error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()

	w := bufio.NewWriterSize(f, 64*1024)
	startSeq = c.Seq()

	var hdr [16]byte
	copy(hdr[0:8], snapMagic[:])
	binary.BigEndian.PutUint64(hdr[8:], startSeq)
	if _, err = w.Write(hdr[:]); err != nil {
		return 0, err
	}

	c.Export(func(key string, value []byte, expireAt int64) {
		if err != nil {
			return
		}
		_, err = w.Write(encodeRecord(cache.Event{
			Op:       cache.OpSet,
			Key:      key,
			Value:    value,
			ExpireAt: expireAt,
		}))
	})
	if err != nil {
		return 0, err
	}
	if err = w.Flush(); err != nil {
		return 0, err
	}
	if err = f.Sync(); err != nil {
		return 0, err
	}
	if err = f.Close(); err != nil {
		return 0, err
	}
	if err = os.Rename(tmp, path); err != nil {
		return 0, err
	}
	return startSeq, nil
}

func ReadSnapshot(path string, fn func(cache.Event)) (startSeq uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	var hdr [16]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, ErrBadSnapshot
	}
	if [8]byte(hdr[0:8]) != snapMagic {
		return 0, ErrBadSnapshot
	}
	startSeq = binary.BigEndian.Uint64(hdr[8:])
	for {
		ev, err := decodeRecord(r)
		if err != nil {
			return startSeq, nil
		}
		fn(ev)
	}
}
