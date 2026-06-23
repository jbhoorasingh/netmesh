//go:build !windows

package diag

// registry is the complete set of permitted diagnostics on Unix-like hosts.
var registry = map[string]builder{
	"ping": {
		name:      "ping",
		needsHost: true,
		build:     func(h string) []string { return []string{"-c", "4", h} },
	},
	"traceroute": {
		name:      "traceroute",
		needsHost: true,
		build:     func(h string) []string { return []string{"-m", "20", "-w", "2", h} },
	},
	"nslookup": {
		name:      "nslookup",
		needsHost: true,
		build:     func(h string) []string { return []string{h} },
	},
	"netstat": {
		name:      "netstat",
		needsHost: false,
		build:     func(string) []string { return []string{"-an"} },
	},
}
