package main

import (
	"testing"

	"distcache/internal/resp"
)

func TestRenderScalarKinds(t *testing.T) {
	cases := []struct {
		name string
		in   resp.Value
		want string
	}{
		{"simple", resp.Value{Type: '+', Str: "OK"}, "OK"},
		{"error", resp.Value{Type: '-', Str: "ERR boom"}, "(error) ERR boom"},
		{"integer", resp.Value{Type: ':', Int: 42}, "(integer) 42"},
		{"bulk", resp.Value{Type: '$', Bulk: []byte("hi")}, `"hi"`},
		{"nil bulk", resp.Value{Type: '$', IsNil: true}, "(nil)"},
		{"nil array", resp.Value{Type: '*', IsNil: true}, "(nil)"},
		{"empty array", resp.Value{Type: '*'}, "(empty array)"},
		{"unknown", resp.Value{Type: '?'}, "(unknown)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := render(tc.in, 0); got != tc.want {
				t.Fatalf("render = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRenderNestedArray(t *testing.T) {
	v := resp.Value{Type: '*', Array: []resp.Value{
		{Type: '+', Str: "first"},
		{Type: ':', Int: 2},
		{Type: '*', Array: []resp.Value{
			{Type: '$', Bulk: []byte("inner")},
		}},
	}}
	want := "1) first\n2) (integer) 2\n3) 1) \"inner\""
	if got := render(v, 0); got != want {
		t.Fatalf("render nested = %q, want %q", got, want)
	}
}
