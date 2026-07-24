package client

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"distcache/internal/resp"
)

type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

type Client struct {
	mu   sync.Mutex
	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: resp.NewReader(conn), w: resp.NewWriter(conn)}, nil
}

func DialPassword(addr, password string) (*Client, error) {
	c, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) Do(args ...string) (resp.Value, error) {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	return c.doBytes(b)
}

func (c *Client) doBytes(args [][]byte) (resp.Value, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.w.WriteCommand(args...); err != nil {
		return resp.Value{}, err
	}
	v, err := c.r.ReadReply()
	if err != nil {
		return resp.Value{}, err
	}
	if v.Type == '-' {
		return v, &Error{Msg: v.Str}
	}
	return v, nil
}

func (c *Client) Ping() error {
	_, err := c.Do("PING")
	return err
}

func (c *Client) Set(key string, value []byte) error {
	_, err := c.doBytes([][]byte{[]byte("SET"), []byte(key), value})
	return err
}

func (c *Client) SetEX(key string, value []byte, ttl time.Duration) error {
	ms := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	_, err := c.doBytes([][]byte{[]byte("SET"), []byte(key), value, []byte("PX"), []byte(ms)})
	return err
}

func (c *Client) Get(key string) (value []byte, ok bool, err error) {
	v, err := c.Do("GET", key)
	if err != nil {
		return nil, false, err
	}
	if v.IsNil {
		return nil, false, nil
	}
	return v.Bulk, true, nil
}

func (c *Client) Del(keys ...string) (int64, error) {
	return c.intCmd(append([]string{"DEL"}, keys...)...)
}

func (c *Client) Exists(keys ...string) (int64, error) {
	return c.intCmd(append([]string{"EXISTS"}, keys...)...)
}

func (c *Client) Incr(key string) (int64, error) { return c.intCmd("INCR", key) }

func (c *Client) IncrBy(key string, delta int64) (int64, error) {
	return c.intCmd("INCRBY", key, strconv.FormatInt(delta, 10))
}

func (c *Client) Expire(key string, ttl time.Duration) (bool, error) {
	n, err := c.intCmd("EXPIRE", key, strconv.FormatInt(int64(ttl/time.Second), 10))
	return n == 1, err
}

func (c *Client) TTL(key string) (time.Duration, error) {
	n, err := c.intCmd("TTL", key)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return time.Duration(n), nil
	}
	return time.Duration(n) * time.Second, nil
}

func (c *Client) Publish(topic string, msg []byte) (int64, error) {
	v, err := c.doBytes([][]byte{[]byte("PUBLISH"), []byte(topic), msg})
	if err != nil {
		return 0, err
	}
	return v.Int, nil
}

func (c *Client) Lock(key string, ttl time.Duration, owner string) (token string, ok bool, err error) {
	v, err := c.Do("LOCK", key, strconv.FormatInt(int64(ttl/time.Second), 10), owner)
	if err != nil {
		return "", false, err
	}
	if v.IsNil {
		return "", false, nil
	}
	return string(v.Bulk), true, nil
}

func (c *Client) Unlock(key, token string) (bool, error) {
	n, err := c.intCmd("UNLOCK", key, token)
	return n == 1, err
}

func (c *Client) intCmd(args ...string) (int64, error) {
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if v.Type != ':' {
		return 0, fmt.Errorf("client: expected integer reply, got %c", v.Type)
	}
	return v.Int, nil
}
