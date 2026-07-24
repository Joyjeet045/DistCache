package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var ErrProtocol = errors.New("resp: protocol error")

const maxBulkLen = 256 << 20

type Reader struct {
	r *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(rd, 32*1024)}
}

func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return r.ReadCommand()
	}
	if line[0] != '*' {
		return parseInline(line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 {
		return nil, ErrProtocol
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		typ, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(typ) == 0 || typ[0] != '$' {
			return nil, ErrProtocol
		}
		blen, err := strconv.Atoi(string(typ[1:]))
		if err != nil || blen < 0 || blen > maxBulkLen {
			return nil, ErrProtocol
		}
		buf := make([]byte, blen+2)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		args = append(args, buf[:blen])
	}
	return args, nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = trimCRLF(line)
	return line, nil
}

func trimCRLF(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func parseInline(line []byte) ([][]byte, error) {
	fields := strings.Fields(string(line))
	if len(fields) == 0 {
		return nil, ErrProtocol
	}
	out := make([][]byte, len(fields))
	for i, f := range fields {
		out[i] = []byte(f)
	}
	return out, nil
}

type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 32*1024)}
}

func (w *Writer) Flush() error { return w.w.Flush() }

func (w *Writer) WriteSimple(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-%s\r\n", msg)
	return err
}

func (w *Writer) WriteInt(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		return w.WriteNil()
	}
	if _, err := fmt.Fprintf(w.w, "$%d\r\n", len(b)); err != nil {
		return err
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) WriteBulkString(s string) error { return w.WriteBulk([]byte(s)) }

func (w *Writer) WriteNil() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

func (w *Writer) WriteArrayHeader(n int) error {
	if n < 0 {
		_, err := w.w.WriteString("*-1\r\n")
		return err
	}
	_, err := fmt.Fprintf(w.w, "*%d\r\n", n)
	return err
}
