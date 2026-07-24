package cache

type item struct {
	value    []byte
	expireAt int64
}

func (it *item) expired(now int64) bool {
	return it.expireAt != 0 && now >= it.expireAt
}

const entryOverhead = 64

func approxSize(key string, value []byte) int64 {
	return int64(len(key) + len(value) + entryOverhead)
}
