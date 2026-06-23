//go:build windows

package diag

// registry is the complete set of permitted diagnostics on Windows hosts.
var registry = map[string]builder{
	"ping": {
		name:      "ping",
		needsHost: true,
		build:     func(h string) []string { return []string{"-n", "4", h} },
	},
	"traceroute": {
		name:      "tracert",
		needsHost: true,
		build:     func(h string) []string { return []string{"-h", "20", "-w", "2000", h} },
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
