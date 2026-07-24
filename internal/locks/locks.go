package locks

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

type Manager struct {
	mu    sync.Mutex
	locks map[string]*lease
	clock func() int64
}

type lease struct {
	token    string
	owner    string
	expireAt int64
}

func New() *Manager {
	return &Manager{
		locks: make(map[string]*lease),
		clock: func() int64 { return time.Now().UnixNano() },
	}
}

func (m *Manager) Acquire(key, owner string, ttl time.Duration) (token string, ok bool) {
	now := m.clock()
	m.mu.Lock()
	defer m.mu.Unlock()

	if l, held := m.locks[key]; held && now < l.expireAt {
		if l.owner != owner {
			return "", false
		}

	}
	tok := newToken()
	m.locks[key] = &lease{token: tok, owner: owner, expireAt: now + int64(ttl)}
	return tok, true
}

func (m *Manager) Release(key, token string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, held := m.locks[key]
	if !held || l.token != token {
		return false
	}
	delete(m.locks, key)
	return true
}

func (m *Manager) Renew(key, token string, ttl time.Duration) bool {
	now := m.clock()
	m.mu.Lock()
	defer m.mu.Unlock()
	l, held := m.locks[key]
	if !held || l.token != token || now >= l.expireAt {
		return false
	}
	l.expireAt = now + int64(ttl)
	return true
}

func (m *Manager) AcquireBlocking(key, owner string, ttl, wait, retry time.Duration) (string, bool) {
	if retry <= 0 {
		retry = 10 * time.Millisecond
	}
	deadline := time.Now().Add(wait)
	for {
		if tok, ok := m.Acquire(key, owner, ttl); ok {
			return tok, true
		}
		if time.Now().After(deadline) {
			return "", false
		}
		time.Sleep(retry)
	}
}

func (m *Manager) IsLocked(key string) bool {
	now := m.clock()
	m.mu.Lock()
	defer m.mu.Unlock()
	l, held := m.locks[key]
	return held && now < l.expireAt
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
