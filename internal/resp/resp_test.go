package resp

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestReadCommandArray(t *testing.T) {
	in := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := NewReader(strings.NewReader(in))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	got := toStrings(args)
	want := []string{"SET", "foo", "bar"}
	if !equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestReadCommandArrayBinarySafe(t *testing.T) {

	val := "a\r\n\x00b"
	in := "*2\r\n$3\r\nSET\r\n$" + itoa(len(val)) + "\r\n" + val + "\r\n"
	r := NewReader(strings.NewReader(in))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if string(args[1]) != val {
		t.Fatalf("value = %q, want %q", args[1], val)
	}
}

func TestReadCommandInline(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\nSET k v\r\n"))
	first, err := r.ReadCommand()
	if err != nil || !equal(toStrings(first), []string{"PING"}) {
		t.Fatalf("first = %v err=%v", first, err)
	}
	second, err := r.ReadCommand()
	if err != nil || !equal(toStrings(second), []string{"SET", "k", "v"}) {
		t.Fatalf("second = %v err=%v", second, err)
	}
}

func TestReadCommandSkipsBlankLines(t *testing.T) {
	r := NewReader(strings.NewReader("\r\n\r\nPING\r\n"))
	args, err := r.ReadCommand()
	if err != nil || !equal(toStrings(args), []string{"PING"}) {
		t.Fatalf("args = %v err=%v", args, err)
	}
}

func TestReadCommandProtocolErrors(t *testing.T) {
	cases := []string{
		"*abc\r\n",
		"*1\r\n+notbulk\r\n",
		"*1\r\n$xx\r\n",
	}
	for _, in := range cases {
		r := NewReader(strings.NewReader(in))
		if _, err := r.ReadCommand(); err != ErrProtocol {
			t.Fatalf("input %q: err = %v, want ErrProtocol", in, err)
		}
	}
}

func TestWriterRepliesRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_ = w.WriteSimple("OK")
	_ = w.WriteError("ERR boom")
	_ = w.WriteInt(-7)
	_ = w.WriteBulk([]byte("hello"))
	_ = w.WriteBulkString("world")
	_ = w.WriteNil()
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	r := NewReader(&buf)
	assertReply(t, r, Value{Type: '+', Str: "OK"})
	assertReply(t, r, Value{Type: '-', Str: "ERR boom"})
	assertReply(t, r, Value{Type: ':', Int: -7})
	assertReply(t, r, Value{Type: '$', Bulk: []byte("hello")})
	assertReply(t, r, Value{Type: '$', Bulk: []byte("world")})
	assertReply(t, r, Value{Type: '$', IsNil: true})
}

func TestReadReplyArrays(t *testing.T) {

	r := NewReader(strings.NewReader("*2\r\n:1\r\n$-1\r\n"))
	v, err := r.ReadReply()
	if err != nil {
		t.Fatalf("ReadReply: %v", err)
	}
	if v.Type != '*' || len(v.Array) != 2 {
		t.Fatalf("array = %+v", v)
	}
	if v.Array[0].Type != ':' || v.Array[0].Int != 1 {
		t.Fatalf("elem0 = %+v", v.Array[0])
	}
	if v.Array[1].Type != '$' || !v.Array[1].IsNil {
		t.Fatalf("elem1 = %+v", v.Array[1])
	}
}

func TestReadReplyNilArray(t *testing.T) {
	r := NewReader(strings.NewReader("*-1\r\n"))
	v, err := r.ReadReply()
	if err != nil {
		t.Fatalf("ReadReply: %v", err)
	}
	if v.Type != '*' || !v.IsNil {
		t.Fatalf("nil array = %+v", v)
	}
}

func TestWriteCommandEncoding(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteCommandStrings("GET", "k"); err != nil {
		t.Fatalf("WriteCommand: %v", err)
	}
	want := "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	if buf.String() != want {
		t.Fatalf("encoded = %q, want %q", buf.String(), want)
	}
}

func assertReply(t *testing.T, r *Reader, want Value) {
	t.Helper()
	got, err := r.ReadReply()
	if err != nil {
		t.Fatalf("ReadReply: %v", err)
	}
	if got.Type != want.Type || got.Str != want.Str || got.Int != want.Int || got.IsNil != want.IsNil {
		t.Fatalf("reply = %+v, want %+v", got, want)
	}
	if !bytes.Equal(got.Bulk, want.Bulk) {
		t.Fatalf("bulk = %q, want %q", got.Bulk, want.Bulk)
	}
}

func toStrings(b [][]byte) []string {
	out := make([]string, len(b))
	for i, x := range b {
		out[i] = string(x)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func itoa(n int) string { return strconv.Itoa(n) }
