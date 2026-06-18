package diag

import (
	"errors"
	"reflect"
	"testing"

	"netmesh/internal/protocol"
)

func TestValidateHostRejectsInjection(t *testing.T) {
	bad := []string{
		"",              // empty
		"-rf",           // leading dash -> argument injection
		"--version",     // flag injection
		"a;rm -rf /",    // command separator
		"a|b",           // pipe
		"a&b",           // background
		"a b",           // space (second argv)
		"$(whoami)",     // command substitution
		"`id`",          // backtick
		"../etc/passwd", // path traversal / slash
		"a/b",           // slash
		"host:23",       // colon (not a bare hostname; use IP form instead)
		"a\nb",          // newline
	}
	for _, h := range bad {
		if err := validateHost(h); err == nil {
			t.Errorf("validateHost(%q) = nil, want error", h)
		}
	}
}

func TestValidateHostAcceptsValid(t *testing.T) {
	good := []string{
		"example.com",
		"host-1.internal.local",
		"10.10.10.5",
		"127.0.0.1",
		"::1",         // IPv6 loopback (via ParseIP)
		"2001:db8::1", // IPv6 (via ParseIP)
		"a",           // single label
	}
	for _, h := range good {
		if err := validateHost(h); err != nil {
			t.Errorf("validateHost(%q) = %v, want nil", h, err)
		}
	}
}

func TestResolveWhitelist(t *testing.T) {
	if _, _, err := resolve(protocol.DiagRequest{Command: "rm", Args: []string{"-rf", "/"}}); !errors.Is(err, ErrNotWhitelisted) {
		t.Errorf("rm should be ErrNotWhitelisted, got %v", err)
	}
	if _, _, err := resolve(protocol.DiagRequest{Command: "bash", Args: []string{"-c", "id"}}); !errors.Is(err, ErrNotWhitelisted) {
		t.Errorf("bash should be ErrNotWhitelisted, got %v", err)
	}
}

func TestResolveBuildsFixedArgv(t *testing.T) {
	tests := []struct {
		cmd  string
		args []string
		want []string
	}{
		{"ping", []string{"8.8.8.8"}, []string{"-c", "4", "8.8.8.8"}},
		{"traceroute", []string{"example.com"}, []string{"-m", "20", "-w", "2", "example.com"}},
		{"nslookup", []string{"example.com"}, []string{"example.com"}},
		{"netstat", nil, []string{"-an"}},
	}
	for _, tc := range tests {
		name, argv, err := resolve(protocol.DiagRequest{Command: tc.cmd, Args: tc.args})
		if err != nil {
			t.Errorf("resolve(%s) err = %v", tc.cmd, err)
			continue
		}
		if name != tc.cmd {
			t.Errorf("name = %q, want %q", name, tc.cmd)
		}
		if !reflect.DeepEqual(argv, tc.want) {
			t.Errorf("%s argv = %v, want %v", tc.cmd, argv, tc.want)
		}
	}
}

func TestResolveArgCount(t *testing.T) {
	// host-requiring commands need exactly one arg
	if _, _, err := resolve(protocol.DiagRequest{Command: "ping"}); err == nil {
		t.Error("ping with no args should error")
	}
	if _, _, err := resolve(protocol.DiagRequest{Command: "ping", Args: []string{"a", "b"}}); err == nil {
		t.Error("ping with two args should error")
	}
	// netstat must take no args
	if _, _, err := resolve(protocol.DiagRequest{Command: "netstat", Args: []string{"x"}}); err == nil {
		t.Error("netstat with args should error")
	}
}

func TestAllowedOrder(t *testing.T) {
	got := Allowed()
	want := []string{"ping", "traceroute", "nslookup", "netstat"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Allowed() = %v, want %v", got, want)
	}
}
