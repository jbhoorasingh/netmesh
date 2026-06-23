package diag

import (
	"errors"
	"reflect"
	"runtime"
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
	ping := []string{"-c", "4", "8.8.8.8"}
	traceName := "traceroute"
	trace := []string{"-m", "20", "-w", "2", "example.com"}
	if runtime.GOOS == "windows" {
		ping = []string{"-n", "4", "8.8.8.8"}
		traceName = "tracert"
		trace = []string{"-h", "20", "-w", "2000", "example.com"}
	}
	tests := []struct {
		cmd      string
		args     []string
		wantName string
		wantArgv []string
	}{
		{"ping", []string{"8.8.8.8"}, "ping", ping},
		{"traceroute", []string{"example.com"}, traceName, trace},
		{"nslookup", []string{"example.com"}, "nslookup", []string{"example.com"}},
		{"netstat", nil, "netstat", []string{"-an"}},
	}
	for _, tc := range tests {
		name, argv, err := resolve(protocol.DiagRequest{Command: tc.cmd, Args: tc.args})
		if err != nil {
			t.Errorf("resolve(%s) err = %v", tc.cmd, err)
			continue
		}
		if name != tc.wantName {
			t.Errorf("name = %q, want %q", name, tc.wantName)
		}
		if !reflect.DeepEqual(argv, tc.wantArgv) {
			t.Errorf("%s argv = %v, want %v", tc.cmd, argv, tc.wantArgv)
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
