package cache

type Op uint8

const (
	OpSet Op = iota
	OpDel
	OpExpire
	OpFlush
)

func (o Op) String() string {
	switch o {
	case OpSet:
		return "SET"
	case OpDel:
		return "DEL"
	case OpExpire:
		return "EXPIRE"
	case OpFlush:
		return "FLUSH"
	default:
		return "UNKNOWN"
	}
}

type Event struct {
	Seq      uint64
	Op       Op
	Key      string
	Value    []byte
	ExpireAt int64

	ack chan struct{}
}
