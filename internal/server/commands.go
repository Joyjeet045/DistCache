package server

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"distcache/internal/cache"
	"distcache/internal/txn"
)

const version = "1.0.0"

type reply struct {
	kind byte
	s    string
	n    int64
	b    []byte
	arr  []reply
	nilA bool
}

func rSimple(s string) reply        { return reply{kind: 's', s: s} }
func rOK() reply                    { return rSimple("OK") }
func rErr(f string, a ...any) reply { return reply{kind: 'e', s: fmt.Sprintf(f, a...)} }
func rInt(n int64) reply            { return reply{kind: 'i', n: n} }
func rBulk(b []byte) reply {
	if b == nil {
		return rNil()
	}
	return reply{kind: 'b', b: b}
}
func rBulkStr(s string) reply    { return reply{kind: 'b', b: []byte(s)} }
func rNil() reply                { return reply{kind: 'n'} }
func rArray(items []reply) reply { return reply{kind: 'a', arr: items} }
func rNilArray() reply           { return reply{kind: 'a', nilA: true} }

func (c *conn) writeReply(rep reply) error {
	switch rep.kind {
	case 's':
		return c.w.WriteSimple(rep.s)
	case 'e':
		return c.w.WriteError(rep.s)
	case 'i':
		return c.w.WriteInt(rep.n)
	case 'b':
		return c.w.WriteBulk(rep.b)
	case 'n':
		return c.w.WriteNil()
	case 'a':
		if rep.nilA {
			return c.w.WriteArrayHeader(-1)
		}
		if err := c.w.WriteArrayHeader(len(rep.arr)); err != nil {
			return err
		}
		for _, it := range rep.arr {
			if err := c.writeReply(it); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func (c *conn) reply(rep reply) {
	c.wmu.Lock()
	_ = c.writeReply(rep)
	_ = c.w.Flush()
	c.wmu.Unlock()
}

var handlers map[string]func(*conn, [][]byte) reply

func init() {
	handlers = map[string]func(*conn, [][]byte) reply{
		"PING":     cmdPing,
		"ECHO":     cmdEcho,
		"SET":      cmdSet,
		"GET":      cmdGet,
		"DEL":      cmdDel,
		"EXISTS":   cmdExists,
		"INCR":     func(c *conn, a [][]byte) reply { return incrBy(c, a, 1, false) },
		"DECR":     func(c *conn, a [][]byte) reply { return incrBy(c, a, -1, false) },
		"INCRBY":   func(c *conn, a [][]byte) reply { return incrBy(c, a, 0, true) },
		"DECRBY":   func(c *conn, a [][]byte) reply { return incrBy(c, a, 0, true) },
		"MSET":     cmdMSet,
		"MGET":     cmdMGet,
		"EXPIRE":   cmdExpire,
		"PEXPIRE":  cmdPExpire,
		"TTL":      func(c *conn, a [][]byte) reply { return ttl(c, a, false) },
		"PTTL":     func(c *conn, a [][]byte) reply { return ttl(c, a, true) },
		"PERSIST":  cmdPersist,
		"KEYS":     cmdKeys,
		"DBSIZE":   cmdDBSize,
		"TYPE":     cmdType,
		"FLUSHALL": cmdFlushAll,
	}
}

func (c *conn) dispatch(args [][]byte) bool {
	cmd := strings.ToUpper(string(args[0]))

	if c.s.cfg.Password != "" && !c.authed {
		switch cmd {
		case "AUTH", "QUIT", "PING":
		default:
			c.reply(rErr("NOAUTH Authentication required."))
			return true
		}
	}

	if c.inMulti {
		switch cmd {
		case "EXEC", "DISCARD", "MULTI", "WATCH", "RESET", "QUIT":

		default:
			h, ok := handlers[cmd]
			if !ok {
				c.reply(rErr("ERR command '%s' not allowed in MULTI", cmd))
				return true
			}
			argsCopy := args
			_ = c.tx.Queue(func(*cache.Cache) any { return h(c, argsCopy) })
			c.reply(rSimple("QUEUED"))
			return true
		}
	}

	switch cmd {
	case "QUIT":
		c.reply(rOK())
		return false
	case "AUTH":
		c.reply(c.cmdAuth(args))
		return true
	case "MULTI":
		c.reply(c.beginMulti())
		return true
	case "EXEC":
		c.reply(c.execTx())
		return true
	case "DISCARD":
		c.reply(c.discard())
		return true
	case "WATCH":
		c.reply(c.watch(args))
		return true
	case "UNWATCH":
		c.reply(c.unwatch())
		return true
	case "SUBSCRIBE":
		return c.doSubscribe(args)
	case "UNSUBSCRIBE":
		return c.doUnsubscribe(args)
	case "PUBLISH":
		c.reply(c.cmdPublish(args))
		return true
	case "LOCK":
		c.reply(c.cmdLock(args))
		return true
	case "UNLOCK":
		c.reply(c.cmdUnlock(args))
		return true
	case "RENEWLOCK":
		c.reply(c.cmdRenewLock(args))
		return true
	case "INFO":
		c.reply(c.cmdInfo())
		return true
	case "CLUSTER":
		c.reply(c.cmdCluster(args))
		return true
	case "COMMAND":
		c.reply(rArray(nil))
		return true
	default:
		if h, ok := handlers[cmd]; ok {
			c.reply(h(c, args))
			return true
		}
		c.reply(rErr("ERR unknown command '%s'", cmd))
		return true
	}
}

func cmdPing(_ *conn, args [][]byte) reply {
	if len(args) >= 2 {
		return rBulk(args[1])
	}
	return rSimple("PONG")
}

func cmdEcho(_ *conn, args [][]byte) reply {
	if len(args) != 2 {
		return rErr("ERR wrong number of arguments for 'echo'")
	}
	return rBulk(args[1])
}

func cmdSet(c *conn, args [][]byte) reply {
	if len(args) < 3 {
		return rErr("ERR wrong number of arguments for 'set'")
	}
	key := string(args[1])
	val := args[2]
	var ttl time.Duration
	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "EX", "PX":
			if i+1 >= len(args) {
				return rErr("ERR syntax error")
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n <= 0 {
				return rErr("ERR invalid expire time in 'set'")
			}
			if opt == "EX" {
				ttl = time.Duration(n) * time.Second
			} else {
				ttl = time.Duration(n) * time.Millisecond
			}
			i++
		default:
			return rErr("ERR syntax error")
		}
	}
	c.s.cache.SetTTL(key, val, ttl)
	return rOK()
}

func cmdGet(c *conn, args [][]byte) reply {
	if len(args) != 2 {
		return rErr("ERR wrong number of arguments for 'get'")
	}
	v, ok := c.s.cache.Get(string(args[1]))
	if !ok {
		return rNil()
	}
	return rBulk(v)
}

func cmdDel(c *conn, args [][]byte) reply {
	if len(args) < 2 {
		return rErr("ERR wrong number of arguments for 'del'")
	}
	var n int64
	for _, k := range args[1:] {
		if c.s.cache.Delete(string(k)) {
			n++
		}
	}
	return rInt(n)
}

func cmdExists(c *conn, args [][]byte) reply {
	if len(args) < 2 {
		return rErr("ERR wrong number of arguments for 'exists'")
	}
	var n int64
	for _, k := range args[1:] {
		if c.s.cache.Exists(string(k)) {
			n++
		}
	}
	return rInt(n)
}

func incrBy(c *conn, args [][]byte, step int64, explicit bool) reply {
	name := strings.ToUpper(string(args[0]))
	var delta int64
	if explicit {
		if len(args) != 3 {
			return rErr("ERR wrong number of arguments for '%s'", strings.ToLower(name))
		}
		n, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return rErr("ERR value is not an integer or out of range")
		}
		delta = n
		if name == "DECRBY" {
			delta = -n
		}
	} else {
		if len(args) != 2 {
			return rErr("ERR wrong number of arguments for '%s'", strings.ToLower(name))
		}
		delta = step
	}
	v, err := c.s.cache.IncrBy(string(args[1]), delta)
	if err == cache.ErrNotInteger {
		return rErr("ERR value is not an integer or out of range")
	}
	if err != nil {
		return rErr("ERR %v", err)
	}
	return rInt(v)
}

func cmdMSet(c *conn, args [][]byte) reply {
	if len(args) < 3 || len(args)%2 == 0 {
		return rErr("ERR wrong number of arguments for 'mset'")
	}
	pairs := make([]cache.KV, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		pairs = append(pairs, cache.KV{Key: string(args[i]), Value: args[i+1]})
	}
	c.s.cache.MSet(pairs)
	return rOK()
}

func cmdMGet(c *conn, args [][]byte) reply {
	if len(args) < 2 {
		return rErr("ERR wrong number of arguments for 'mget'")
	}
	keys := make([]string, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = string(k)
	}
	vals, found := c.s.cache.MGet(keys)
	items := make([]reply, len(keys))
	for i := range keys {
		if found[i] {
			items[i] = rBulk(vals[i])
		} else {
			items[i] = rNil()
		}
	}
	return rArray(items)
}

func cmdExpire(c *conn, args [][]byte) reply  { return expire(c, args, time.Second) }
func cmdPExpire(c *conn, args [][]byte) reply { return expire(c, args, time.Millisecond) }

func expire(c *conn, args [][]byte, unit time.Duration) reply {
	if len(args) != 3 {
		return rErr("ERR wrong number of arguments")
	}
	n, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return rErr("ERR value is not an integer or out of range")
	}
	if c.s.cache.Expire(string(args[1]), time.Duration(n)*unit) {
		return rInt(1)
	}
	return rInt(0)
}

func cmdPersist(c *conn, args [][]byte) reply {
	if len(args) != 2 {
		return rErr("ERR wrong number of arguments for 'persist'")
	}

	if c.s.cache.Expire(string(args[1]), 0) {
		return rInt(1)
	}
	return rInt(0)
}

func ttl(c *conn, args [][]byte, millis bool) reply {
	if len(args) != 2 {
		return rErr("ERR wrong number of arguments")
	}
	rem, found, persists := c.s.cache.TTL(string(args[1]))
	switch {
	case !found:
		return rInt(-2)
	case persists:
		return rInt(-1)
	default:
		if millis {
			return rInt(int64(rem / time.Millisecond))
		}
		return rInt(int64(rem / time.Second))
	}
}

func cmdKeys(c *conn, args [][]byte) reply {
	pattern := "*"
	if len(args) >= 2 {
		pattern = string(args[1])
	}
	var items []reply
	for _, k := range c.s.cache.Keys() {
		if pattern == "*" {
			items = append(items, rBulkStr(k))
			continue
		}
		if ok, _ := filepath.Match(pattern, k); ok {
			items = append(items, rBulkStr(k))
		}
	}
	return rArray(items)
}

func cmdDBSize(c *conn, _ [][]byte) reply { return rInt(int64(c.s.cache.Len())) }

func cmdType(c *conn, args [][]byte) reply {
	if len(args) != 2 {
		return rErr("ERR wrong number of arguments for 'type'")
	}
	if c.s.cache.Exists(string(args[1])) {
		return rSimple("string")
	}
	return rSimple("none")
}

func cmdFlushAll(c *conn, _ [][]byte) reply {
	c.s.cache.FlushAll()
	return rOK()
}

func (c *conn) cmdAuth(args [][]byte) reply {
	if len(args) != 2 {
		return rErr("ERR wrong number of arguments for 'auth'")
	}
	if c.s.cfg.Password == "" {
		return rErr("ERR Client sent AUTH, but no password is set")
	}
	if string(args[1]) != c.s.cfg.Password {
		return rErr("WRONGPASS invalid username-password pair")
	}
	c.authed = true
	return rOK()
}

func (c *conn) beginMulti() reply {
	if c.inMulti {
		return rErr("ERR MULTI calls can not be nested")
	}
	if c.tx == nil {
		c.tx = c.s.txn.Begin()
	}
	_ = c.tx.Multi()
	c.inMulti = true
	return rOK()
}

func (c *conn) execTx() reply {
	if !c.inMulti {
		return rErr("ERR EXEC without MULTI")
	}
	c.inMulti = false
	results, err := c.tx.Exec()
	c.tx = nil
	if err == txn.ErrAborted {
		return rNilArray()
	}
	if err != nil {
		return rErr("ERR %v", err)
	}
	items := make([]reply, len(results))
	for i, r := range results {
		items[i] = r.(reply)
	}
	return rArray(items)
}

func (c *conn) discard() reply {
	if c.tx == nil {
		return rErr("ERR DISCARD without MULTI")
	}
	c.tx.Discard()
	c.tx = nil
	c.inMulti = false
	return rOK()
}

func (c *conn) watch(args [][]byte) reply {
	if c.inMulti {
		return rErr("ERR WATCH inside MULTI is not allowed")
	}
	if len(args) < 2 {
		return rErr("ERR wrong number of arguments for 'watch'")
	}
	if c.tx == nil {
		c.tx = c.s.txn.Begin()
	}
	keys := make([]string, len(args)-1)
	for i, k := range args[1:] {
		keys[i] = string(k)
	}
	_ = c.tx.Watch(keys...)
	return rOK()
}

func (c *conn) unwatch() reply {
	if c.tx != nil && !c.inMulti {
		c.tx.Discard()
		c.tx = nil
	}
	return rOK()
}

func (c *conn) cmdLock(args [][]byte) reply {
	if len(args) < 3 {
		return rErr("ERR usage: LOCK key ttlSeconds [owner]")
	}
	secs, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || secs <= 0 {
		return rErr("ERR invalid ttl")
	}
	owner := c.nc.RemoteAddr().String()
	if len(args) >= 4 {
		owner = string(args[3])
	}
	tok, ok := c.s.locks.Acquire(string(args[1]), owner, time.Duration(secs)*time.Second)
	if !ok {
		return rNil()
	}
	return rBulkStr(tok)
}

func (c *conn) cmdUnlock(args [][]byte) reply {
	if len(args) != 3 {
		return rErr("ERR usage: UNLOCK key token")
	}
	if c.s.locks.Release(string(args[1]), string(args[2])) {
		return rInt(1)
	}
	return rInt(0)
}

func (c *conn) cmdRenewLock(args [][]byte) reply {
	if len(args) != 4 {
		return rErr("ERR usage: RENEWLOCK key token ttlSeconds")
	}
	secs, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil || secs <= 0 {
		return rErr("ERR invalid ttl")
	}
	if c.s.locks.Renew(string(args[1]), string(args[2]), time.Duration(secs)*time.Second) {
		return rInt(1)
	}
	return rInt(0)
}

func (c *conn) cmdInfo() reply {
	st := c.s.cache.Stats()
	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\n")
	fmt.Fprintf(&b, "distcache_version:%s\r\n", version)
	fmt.Fprintf(&b, "node_id:%s\r\n", c.s.cfg.NodeID)
	fmt.Fprintf(&b, "go_version:%s\r\n", runtime.Version())
	fmt.Fprintf(&b, "# Stats\r\n")
	fmt.Fprintf(&b, "keyspace_hits:%d\r\n", st.Hits)
	fmt.Fprintf(&b, "keyspace_misses:%d\r\n", st.Misses)
	fmt.Fprintf(&b, "hit_ratio:%.4f\r\n", st.HitRatio())
	fmt.Fprintf(&b, "evictions:%d\r\n", st.Evictions)
	fmt.Fprintf(&b, "expirations:%d\r\n", st.Expirations)
	fmt.Fprintf(&b, "keys:%d\r\n", st.Keys)
	fmt.Fprintf(&b, "memory_bytes:%d\r\n", st.MemoryBytes)
	fmt.Fprintf(&b, "# Replication\r\n")
	switch {
	case c.s.replica != nil:
		fmt.Fprintf(&b, "role:replica\r\n")
		fmt.Fprintf(&b, "master_addr:%s\r\n", c.s.replica.PrimaryAddr())
		fmt.Fprintf(&b, "master_link_status:%s\r\n", linkStatus(c.s.replica.Synced()))
		fmt.Fprintf(&b, "slave_repl_offset:%d\r\n", c.s.replica.LastApplied())
	case c.s.primary != nil:
		fmt.Fprintf(&b, "role:master\r\n")
		reps := c.s.primary.Replicas()
		fmt.Fprintf(&b, "connected_slaves:%d\r\n", len(reps))
		for i, rp := range reps {
			fmt.Fprintf(&b, "slave%d:addr=%s,sent_seq=%d,ack_seq=%d\r\n", i, rp.Addr, rp.LastSentSeq, rp.LastAckSeq)
		}
	default:
		fmt.Fprintf(&b, "role:master\r\n")
		fmt.Fprintf(&b, "connected_slaves:0\r\n")
	}
	return rBulkStr(b.String())
}

func linkStatus(synced bool) string {
	if synced {
		return "up"
	}
	return "down"
}

func (c *conn) cmdCluster(args [][]byte) reply {
	sub := "INFO"
	if len(args) >= 2 {
		sub = strings.ToUpper(string(args[1]))
	}
	switch sub {
	case "MYID":
		return rBulkStr(c.s.cfg.NodeID)
	case "INFO":
		enabled, known := 0, 1
		if c.s.registry != nil {
			enabled = 1
			known = c.s.registry.Size()
		}
		return rBulkStr(fmt.Sprintf("cluster_enabled:%d\r\ncluster_known_nodes:%d\r\n", enabled, known))
	case "NODES":
		if c.s.registry == nil {
			return rBulkStr(fmt.Sprintf("%s %s myself\r\n", c.s.cfg.NodeID, c.nc.LocalAddr().String()))
		}
		var b strings.Builder
		for _, n := range c.s.registry.Nodes() {
			flags := "master"
			if n.Self {
				flags = "myself,master"
			}
			state := "connected"
			if !n.Alive {
				state = "disconnected"
			}
			fmt.Fprintf(&b, "%s %s %s %s\r\n", n.ID, n.Addr, flags, state)
		}
		return rBulkStr(b.String())
	default:
		return rErr("ERR unknown CLUSTER subcommand '%s'", sub)
	}
}
