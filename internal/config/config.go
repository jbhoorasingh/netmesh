// Package config parses NetMesh command-line arguments into a validated
// runtime configuration and determines the operating mode of the single
// netmesh binary.
package config

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Mode is the role the binary runs as, derived from the -master flag.
type Mode int

const (
	// ModeController runs the Controller (Master): -master=self.
	ModeController Mode = iota
	// ModeAgentJoin runs an Agent that immediately joins a known controller:
	// -master=<IP>.
	ModeAgentJoin
	// ModeAgentHolding runs an Agent with no controller yet (no -master flag).
	// It serves a local UI that prompts the operator to enter the Master IP.
	ModeAgentHolding
)

func (m Mode) String() string {
	switch m {
	case ModeController:
		return "controller"
	case ModeAgentJoin:
		return "agent-join"
	case ModeAgentHolding:
		return "agent-holding"
	default:
		return "unknown"
	}
}

// DefaultPort is the single port used for Web UI, REST API, and WebSocket
// control plane on both Master and Agents.
const DefaultPort = 5999

// Config is the fully-parsed, validated runtime configuration.
type Config struct {
	Mode Mode
	Port int

	// MasterAddr is the controller host the agent joins (ModeAgentJoin only).
	MasterAddr string

	// Auth (controller only). When AuthEnabled is false the Controller UI grants
	// full access to everyone; when true, anonymous users get read-only access
	// and privileged actions require these credentials.
	AuthEnabled bool
	AdminUser   string
	AdminPass   string

	// AgentID is the stable identity this node advertises. Defaults to the
	// hostname; overridable for tests / multi-node-per-host setups.
	AgentID string

	// Token is the optional shared secret for the agent control plane. On the
	// Controller it is required of every joining agent; on an Agent it is
	// presented when joining. Empty means an open control plane.
	Token string

	// DataPort is the agent's data-plane port: the UDP/TCP echo Responder binds
	// it, and peers target it for probes. Defaults to Port+1.
	DataPort int
}

// rawFlags holds the unparsed flag values so we can apply the slightly unusual
// -master semantics after parsing.
type rawFlags struct {
	master   string
	admin    string
	port     int
	dataPort int
	id       string
	token    string
}

// Parse parses the given arguments (typically os.Args[1:]) into a Config.
// progName is used for usage output. It returns flag.ErrHelp when -h/-help was
// requested so the caller can exit cleanly.
func Parse(progName string, args []string) (*Config, error) {
	fs := flag.NewFlagSet(progName, flag.ContinueOnError)
	var rf rawFlags
	fs.StringVar(&rf.master, "master", "",
		`Controller selection:
        self        run as the Controller (Master)
        <IP/host>   run as an Agent and join that Controller
        (omitted)   run as an Agent in holding state (join later via the UI)`)
	fs.StringVar(&rf.admin, "admin", "",
		"Secure the Controller with credentials in user:pass form (enables RBAC). "+
			"Omit for an open Controller.")
	fs.IntVar(&rf.port, "port", DefaultPort,
		"TCP port for Web UI, API and WebSocket on Master and Agents.")
	fs.IntVar(&rf.dataPort, "data-port", 0,
		"Data-plane UDP/TCP echo port for agent-to-agent probes (default: -port + 1).")
	fs.StringVar(&rf.id, "id", "",
		"Override this node's advertised agent ID (defaults to hostname).")
	fs.StringVar(&rf.token, "token", "",
		"Optional shared secret for the agent control plane. On the Controller it "+
			"is required of every joining agent; on an Agent it is presented when joining.")

	if err := fs.Parse(args); err != nil {
		return nil, err // includes flag.ErrHelp
	}
	return build(rf)
}

// build validates raw flags and resolves the operating mode.
func build(rf rawFlags) (*Config, error) {
	if rf.port < 1 || rf.port > 65535 {
		return nil, fmt.Errorf("invalid -port %d: must be 1..65535", rf.port)
	}
	dataPort := rf.dataPort
	if dataPort == 0 {
		dataPort = rf.port + 1
		if dataPort > 65535 {
			dataPort = rf.port - 1
		}
	}
	if dataPort < 1 || dataPort > 65535 {
		return nil, fmt.Errorf("invalid -data-port %d: must be 1..65535", dataPort)
	}

	cfg := &Config{Port: rf.port, DataPort: dataPort, Token: rf.token}

	cfg.AgentID = rf.id
	if cfg.AgentID == "" {
		if hn, err := os.Hostname(); err == nil && hn != "" {
			cfg.AgentID = hn
		} else {
			cfg.AgentID = "agent-" + strconv.Itoa(rf.port)
		}
	}

	switch strings.TrimSpace(strings.ToLower(rf.master)) {
	case "self":
		cfg.Mode = ModeController
	case "":
		cfg.Mode = ModeAgentHolding
	default:
		cfg.Mode = ModeAgentJoin
		addr, err := normaliseMaster(rf.master, rf.port)
		if err != nil {
			return nil, err
		}
		cfg.MasterAddr = addr
	}

	// -admin is only meaningful on the Controller, but we parse it regardless so
	// a mis-placed flag is a clear error rather than a silent no-op.
	if rf.admin != "" {
		if cfg.Mode != ModeController {
			return nil, errors.New("-admin is only valid with -master=self (Controller mode)")
		}
		user, pass, err := parseAdmin(rf.admin)
		if err != nil {
			return nil, err
		}
		cfg.AuthEnabled = true
		cfg.AdminUser = user
		cfg.AdminPass = pass
	}

	return cfg, nil
}

// normaliseMaster accepts "host", "host:port", or an IP and returns a
// host:port suitable for dialling, defaulting the port to the agent's own port.
func normaliseMaster(master string, defaultPort int) (string, error) {
	master = strings.TrimSpace(master)
	// Strip an accidental scheme.
	master = strings.TrimPrefix(master, "ws://")
	master = strings.TrimPrefix(master, "http://")

	if host, port, err := net.SplitHostPort(master); err == nil {
		if host == "" {
			return "", fmt.Errorf("invalid -master %q: empty host", master)
		}
		return net.JoinHostPort(host, port), nil
	}
	// No explicit port: use the default.
	if master == "" {
		return "", errors.New("invalid -master: empty value")
	}
	return net.JoinHostPort(master, strconv.Itoa(defaultPort)), nil
}

// parseAdmin splits "user:pass" allowing colons inside the password.
func parseAdmin(s string) (user, pass string, err error) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", "", errors.New("-admin must be in user:pass form")
	}
	user = s[:i]
	pass = s[i+1:]
	if user == "" || pass == "" {
		return "", "", errors.New("-admin user and pass must both be non-empty")
	}
	return user, pass, nil
}

// ListenAddr is the address the local HTTP/WS server binds to.
func (c *Config) ListenAddr() string {
	return fmt.Sprintf(":%d", c.Port)
}
