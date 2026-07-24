package server

import (
	"net"
	"strings"
	"testing"
	"time"

	"distcache/internal/resp"
)

func mustDo(t *testing.T, c interface {
	Do(...string) (resp.Value, error)
}, args ...string) resp.Value {
	t.Helper()
	v, err := c.Do(args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

func TestServerEchoPersistDecr(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t"})
	c := dial(t, addr)

	if v := mustDo(t, c, "ECHO", "hi there"); string(v.Bulk) != "hi there" {
		t.Fatalf("ECHO = %q", v.Bulk)
	}

	if v := mustDo(t, c, "DECR", "ctr"); v.Int != -1 {
		t.Fatalf("DECR = %d, want -1", v.Int)
	}
	if v := mustDo(t, c, "DECRBY", "ctr", "5"); v.Int != -6 {
		t.Fatalf("DECRBY = %d, want -6", v.Int)
	}

	mustDo(t, c, "SET", "k", "v")
	if v := mustDo(t, c, "PEXPIRE", "k", "50000"); v.Int != 1 {
		t.Fatalf("PEXPIRE = %d, want 1", v.Int)
	}
	if v := mustDo(t, c, "PTTL", "k"); v.Int <= 0 || v.Int > 50000 {
		t.Fatalf("PTTL = %d, want (0,50000]", v.Int)
	}
	if v := mustDo(t, c, "PERSIST", "k"); v.Int != 1 {
		t.Fatalf("PERSIST = %d, want 1", v.Int)
	}
	if v := mustDo(t, c, "PTTL", "k"); v.Int != -1 {
		t.Fatalf("PTTL after persist = %d, want -1", v.Int)
	}
	if v := mustDo(t, c, "PTTL", "missing"); v.Int != -2 {
		t.Fatalf("PTTL missing = %d, want -2", v.Int)
	}
}

func TestServerMultiKeyCommands(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t"})
	c := dial(t, addr)

	mustDo(t, c, "MSET", "a", "1", "b", "2", "c", "3")
	if v := mustDo(t, c, "DBSIZE"); v.Int != 3 {
		t.Fatalf("DBSIZE = %d, want 3", v.Int)
	}

	mget := mustDo(t, c, "MGET", "a", "missing", "c")
	if mget.Type != '*' || len(mget.Array) != 3 {
		t.Fatalf("MGET = %+v", mget)
	}
	if string(mget.Array[0].Bulk) != "1" || !mget.Array[1].IsNil || string(mget.Array[2].Bulk) != "3" {
		t.Fatalf("MGET values = %+v", mget.Array)
	}

	if v := mustDo(t, c, "TYPE", "a"); v.Str != "string" {
		t.Fatalf("TYPE a = %q", v.Str)
	}
	if v := mustDo(t, c, "TYPE", "missing"); v.Str != "none" {
		t.Fatalf("TYPE missing = %q", v.Str)
	}

	keys := mustDo(t, c, "KEYS", "*")
	if keys.Type != '*' || len(keys.Array) != 3 {
		t.Fatalf("KEYS = %+v", keys)
	}

	one := mustDo(t, c, "KEYS", "a")
	if len(one.Array) != 1 || string(one.Array[0].Bulk) != "a" {
		t.Fatalf("KEYS a = %+v", one.Array)
	}

	mustDo(t, c, "FLUSHALL")
	if v := mustDo(t, c, "DBSIZE"); v.Int != 0 {
		t.Fatalf("DBSIZE after flush = %d, want 0", v.Int)
	}
}

func TestServerSetExpiryOptions(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t"})
	c := dial(t, addr)

	mustDo(t, c, "SET", "ek", "v", "EX", "100")
	if v := mustDo(t, c, "TTL", "ek"); v.Int <= 0 || v.Int > 100 {
		t.Fatalf("TTL = %d, want (0,100]", v.Int)
	}

	mustDo(t, c, "SET", "px", "v", "PX", "30")
	deadline := time.Now().Add(2 * time.Second)
	for {
		v := mustDo(t, c, "GET", "px")
		if v.IsNil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PX key did not expire")
		}
	}

	if _, err := c.Do("SET", "bad", "v", "EX", "0"); err == nil {
		t.Fatal("SET EX 0 should error")
	}
	if _, err := c.Do("SET", "bad", "v", "BOGUS", "1"); err == nil {
		t.Fatal("SET with unknown option should error")
	}
}

func TestServerWatchExecAbort(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t"})
	w := dial(t, addr)
	m := dial(t, addr)

	mustDo(t, w, "SET", "wk", "v0")
	mustDo(t, w, "WATCH", "wk")
	mustDo(t, w, "MULTI")
	if v := mustDo(t, w, "SET", "wk", "v1"); v.Str != "QUEUED" {
		t.Fatalf("queued reply = %+v", v)
	}

	mustDo(t, m, "SET", "wk", "v2")

	mustDo(t, m, "WATCH", "__barrier__")

	exec := mustDo(t, w, "EXEC")
	if exec.Type != '*' || !exec.IsNil {
		t.Fatalf("EXEC after conflict = %+v, want nil array", exec)
	}

	if v := mustDo(t, w, "GET", "wk"); string(v.Bulk) != "v2" {
		t.Fatalf("value = %q, want v2 (aborted)", v.Bulk)
	}
}

func TestServerWatchExecSuccess(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t"})
	c := dial(t, addr)

	mustDo(t, c, "SET", "sk", "v0")
	mustDo(t, c, "WATCH", "sk")
	mustDo(t, c, "MULTI")
	mustDo(t, c, "SET", "sk", "v1")
	exec := mustDo(t, c, "EXEC")
	if exec.Type != '*' || exec.IsNil || len(exec.Array) != 1 {
		t.Fatalf("EXEC = %+v, want 1-element array", exec)
	}
	if v := mustDo(t, c, "GET", "sk"); string(v.Bulk) != "v1" {
		t.Fatalf("value = %q, want v1", v.Bulk)
	}
}

func TestServerSubscribeRouting(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t"})

	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	rr := resp.NewReader(nc)
	rw := resp.NewWriter(nc)

	if err := rw.WriteCommandStrings("SUBSCRIBE", "t1", "t2"); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = rw.Flush()
	for i, want := range []struct {
		topic string
		count int64
	}{{"t1", 1}, {"t2", 2}} {
		v := readReply(t, rr)
		if len(v.Array) != 3 || string(v.Array[0].Bulk) != "subscribe" ||
			string(v.Array[1].Bulk) != want.topic || v.Array[2].Int != want.count {
			t.Fatalf("confirm %d = %+v", i, v.Array)
		}
	}

	pub := dial(t, addr)
	if v := mustDo(t, pub, "PUBLISH", "t1", "hello"); v.Int != 1 {
		t.Fatalf("PUBLISH t1 receivers = %d, want 1", v.Int)
	}
	if v := mustDo(t, pub, "PUBLISH", "t2", "world"); v.Int != 1 {
		t.Fatalf("PUBLISH t2 receivers = %d, want 1", v.Int)
	}

	got := map[string]string{}
	for i := 0; i < 2; i++ {
		v := readReply(t, rr)
		if len(v.Array) != 3 || string(v.Array[0].Bulk) != "message" {
			t.Fatalf("message %d = %+v", i, v.Array)
		}
		got[string(v.Array[1].Bulk)] = string(v.Array[2].Bulk)
	}
	if got["t1"] != "hello" || got["t2"] != "world" {
		t.Fatalf("routed messages = %v", got)
	}

	_ = rw.WriteCommandStrings("UNSUBSCRIBE", "t1")
	_ = rw.Flush()
	v := readReply(t, rr)
	if string(v.Array[0].Bulk) != "unsubscribe" || string(v.Array[1].Bulk) != "t1" || v.Array[2].Int != 1 {
		t.Fatalf("unsubscribe t1 = %+v", v.Array)
	}

	if r := mustDo(t, pub, "PUBLISH", "t1", "x"); r.Int != 0 {
		t.Fatalf("PUBLISH t1 after unsub = %d, want 0", r.Int)
	}

	_ = rw.WriteCommandStrings("UNSUBSCRIBE")
	_ = rw.Flush()
	v = readReply(t, rr)
	if string(v.Array[0].Bulk) != "unsubscribe" || v.Array[2].Int != 0 {
		t.Fatalf("unsubscribe all = %+v", v.Array)
	}
}

func TestServerIntrospectionAndLocks(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "node-x"})
	c := dial(t, addr)

	info := mustDo(t, c, "INFO")
	if info.Type != '$' || !strings.Contains(string(info.Bulk), "role:master") ||
		!strings.Contains(string(info.Bulk), "node-x") {
		t.Fatalf("INFO missing sections: %q", info.Bulk)
	}

	nodes := mustDo(t, c, "CLUSTER", "NODES")
	if nodes.Type != '$' || !strings.Contains(string(nodes.Bulk), "myself") {
		t.Fatalf("CLUSTER NODES = %q", nodes.Bulk)
	}
	cinfo := mustDo(t, c, "CLUSTER", "INFO")
	if !strings.Contains(string(cinfo.Bulk), "cluster_enabled") {
		t.Fatalf("CLUSTER INFO = %q", cinfo.Bulk)
	}
	myid := mustDo(t, c, "CLUSTER", "MYID")
	if string(myid.Bulk) != "node-x" {
		t.Fatalf("CLUSTER MYID = %q", myid.Bulk)
	}

	cmd := mustDo(t, c, "COMMAND")
	if cmd.Type != '*' {
		t.Fatalf("COMMAND = %+v", cmd)
	}

	lock := mustDo(t, c, "LOCK", "res", "30", "owner")
	if lock.Type != '$' || lock.IsNil {
		t.Fatalf("LOCK = %+v", lock)
	}
	token := string(lock.Bulk)
	if v := mustDo(t, c, "RENEWLOCK", "res", token, "60"); v.Int != 1 {
		t.Fatalf("RENEWLOCK = %d, want 1", v.Int)
	}
	if v := mustDo(t, c, "RENEWLOCK", "res", "wrong-token", "60"); v.Int != 0 {
		t.Fatalf("RENEWLOCK wrong token = %d, want 0", v.Int)
	}
	if v := mustDo(t, c, "UNLOCK", "res", token); v.Int != 1 {
		t.Fatalf("UNLOCK = %d, want 1", v.Int)
	}
}

func TestServerAuthWrongPassword(t *testing.T) {
	addr := newTestServer(t, Config{NodeID: "t", Password: "secret"})
	c := dial(t, addr)

	if _, err := c.Do("AUTH", "wrong"); err == nil {
		t.Fatal("AUTH with wrong password should error")
	}

	if _, err := c.Do("GET", "k"); err == nil {
		t.Fatal("command before auth should be rejected")
	}
	if v := mustDo(t, c, "AUTH", "secret"); v.Str != "OK" {
		t.Fatalf("AUTH = %+v", v)
	}
	mustDo(t, c, "SET", "k", "v")
	if v := mustDo(t, c, "GET", "k"); string(v.Bulk) != "v" {
		t.Fatalf("GET after auth = %q", v.Bulk)
	}
}

func readReply(t *testing.T, r *resp.Reader) resp.Value {
	t.Helper()
	v, err := r.ReadReply()
	if err != nil {
		t.Fatalf("ReadReply: %v", err)
	}
	return v
}
