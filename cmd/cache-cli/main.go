package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"distcache/internal/resp"
	"distcache/pkg/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	password := flag.String("password", "", "AUTH password")
	flag.Parse()

	c, err := client.DialPassword(*addr, *password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	if args := flag.Args(); len(args) > 0 {
		v, err := c.Do(args...)
		if err != nil {
			if _, ok := err.(*client.Error); !ok {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				os.Exit(1)
			}
		}
		fmt.Println(render(v, 0))
		return
	}

	fmt.Printf("connected to %s — type commands, 'quit' to exit\n", *addr)
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for {
		fmt.Printf("%s> ", *addr)
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			break
		}
		fields := strings.Fields(line)
		v, err := c.Do(fields...)
		if err != nil {
			if _, ok := err.(*client.Error); !ok {
				fmt.Fprintf(os.Stderr, "(error) %v\n", err)
				continue
			}
		}
		fmt.Println(render(v, 0))
	}
}

func render(v resp.Value, depth int) string {
	switch v.Type {
	case '+':
		return v.Str
	case '-':
		return "(error) " + v.Str
	case ':':
		return fmt.Sprintf("(integer) %d", v.Int)
	case '$':
		if v.IsNil {
			return "(nil)"
		}
		return fmt.Sprintf("%q", string(v.Bulk))
	case '*':
		if v.IsNil {
			return "(nil)"
		}
		if len(v.Array) == 0 {
			return "(empty array)"
		}
		var b strings.Builder
		for i, e := range v.Array {
			if i > 0 {
				b.WriteByte('\n')
			}
			fmt.Fprintf(&b, "%d) %s", i+1, render(e, depth+1))
		}
		return b.String()
	default:
		return "(unknown)"
	}
}
