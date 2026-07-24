package resp

import (
	"fmt"
	"io"
	"strconv"
)

type Value struct {
	Type  byte
	Str   string
	Int   int64
	Bulk  []byte
	IsNil bool
	Array []Value
}

func (w *Writer) WriteCommand(args ...[]byte) error {
	if err := w.WriteArrayHeader(len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w.w, "$%d\r\n", len(a)); err != nil {
			return err
		}
		if _, err := w.w.Write(a); err != nil {
			return err
		}
		if _, err := w.w.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (w *Writer) WriteCommandStrings(args ...string) error {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	return w.WriteCommand(b...)
}

func (r *Reader) ReadReply() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, ErrProtocol
	}
	typ, rest := line[0], string(line[1:])
	switch typ {
	case '+':
		return Value{Type: '+', Str: rest}, nil
	case '-':
		return Value{Type: '-', Str: rest}, nil
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return Value{}, ErrProtocol
		}
		return Value{Type: ':', Int: n}, nil
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return Value{}, ErrProtocol
		}
		if n < 0 {
			return Value{Type: '$', IsNil: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return Value{}, err
		}
		return Value{Type: '$', Bulk: buf[:n]}, nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return Value{}, ErrProtocol
		}
		if n < 0 {
			return Value{Type: '*', IsNil: true}, nil
		}
		arr := make([]Value, n)
		for i := 0; i < n; i++ {
			v, err := r.ReadReply()
			if err != nil {
				return Value{}, err
			}
			arr[i] = v
		}
		return Value{Type: '*', Array: arr}, nil
	default:
		return Value{}, ErrProtocol
	}
}
