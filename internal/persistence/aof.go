package persistence

import (
	"bufio"
	"os"
	"sync"
	"time"

	"distcache/internal/cache"
)

type SyncPolicy string

const (
	SyncNo SyncPolicy = "no"

	SyncEverySec SyncPolicy = "everysec"

	SyncAlways SyncPolicy = "always"
)

type AOF struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	policy SyncPolicy
	path   string

	stop chan struct{}
	wg   sync.WaitGroup
}

func OpenAOF(path string, policy SyncPolicy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		f:      f,
		w:      bufio.NewWriterSize(f, 64*1024),
		policy: policy,
		path:   path,
		stop:   make(chan struct{}),
	}
	if policy == SyncEverySec {
		a.wg.Add(1)
		go a.everySec()
	}
	return a, nil
}

func (a *AOF) Append(ev cache.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.w.Write(encodeRecord(ev)); err != nil {
		return err
	}
	if a.policy == SyncAlways {
		if err := a.w.Flush(); err != nil {
			return err
		}
		return a.f.Sync()
	}
	return nil
}

func (a *AOF) everySec() {
	defer a.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			a.mu.Lock()
			_ = a.w.Flush()
			_ = a.f.Sync()
			a.mu.Unlock()
		case <-a.stop:
			return
		}
	}
}

func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.Flush(); err != nil {
		return err
	}
	return a.f.Sync()
}

func (a *AOF) Close() error {
	close(a.stop)
	a.wg.Wait()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.Flush(); err != nil {
		return err
	}
	if err := a.f.Sync(); err != nil {
		return err
	}
	return a.f.Close()
}

func (a *AOF) Compact(minSeqExclusive uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.w.Flush(); err != nil {
		return err
	}
	if err := a.f.Sync(); err != nil {
		return err
	}

	kept, err := readRecords(a.path, minSeqExclusive)
	if err != nil {
		return err
	}

	tmp := a.path + ".compact"
	tf, err := os.Create(tmp)
	if err != nil {
		return err
	}
	tw := bufio.NewWriterSize(tf, 64*1024)
	for _, ev := range kept {
		if _, err := tw.Write(encodeRecord(ev)); err != nil {
			tf.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		tf.Close()
		os.Remove(tmp)
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		os.Remove(tmp)
		return err
	}
	if err := tf.Close(); err != nil {
		os.Remove(tmp)
		return err
	}

	if err := a.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, a.path); err != nil {
		return err
	}
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	a.f = f
	a.w = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func readRecords(path string, minSeqExclusive uint64) ([]cache.Event, error) {
	var out []cache.Event
	err := ReadAOF(path, func(ev cache.Event) {
		if ev.Seq > minSeqExclusive {
			out = append(out, ev)
		}
	})
	return out, err
}

func ReadAOF(path string, fn func(cache.Event)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64*1024)
	for {
		ev, err := decodeRecord(r)
		if err != nil {
			if err == errTornRecord {
				return nil
			}
			return nil
		}
		fn(ev)
	}
}
