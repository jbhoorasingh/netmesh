// Package diag implements the Agent side of the Master-driven diagnostics
// console. The Controller requests a diagnostic; the Agent runs it ONLY if it
// matches a strict whitelist, then streams the output back over the control
// plane.
//
// Security properties:
//   - No shell is ever invoked. Commands run via exec.Command(name, args...),
//     so shell metacharacters in any field are inert.
//   - The command name must be one of a fixed set; the flags are fixed by this
//     package, never taken from the request.
//   - The single user-supplied value (a target host) is validated against a
//     strict hostname/IP pattern and may not begin with '-', preventing
//     argument injection.
//   - Output volume and wall-clock time are bounded.
//
// This package deliberately exposes NO way to run an arbitrary command.
package diag

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"netmesh/internal/protocol"
)

// Limits on diagnostic execution.
const (
	MaxRuntime   = 30 * time.Second
	MaxOutputB   = 256 << 10 // 256 KiB total streamed output
	chunkScanCap = 64 << 10  // max single line length
)

// ErrNotWhitelisted indicates the requested command is not permitted.
var ErrNotWhitelisted = errors.New("diag: command not whitelisted")

// hostPattern matches DNS hostnames only. It forbids a leading '-' (argument
// injection) and any shell-significant character. IPv4/IPv6 literals are
// accepted separately via net.ParseIP, so ':' is intentionally excluded here.
var hostPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.\-]{0,253}[A-Za-z0-9])?$`)

// builder validates a request and returns the exact argv to execute. The flags
// are owned by this package; only a single validated host may be injected.
type builder struct {
	name      string
	needsHost bool
	build     func(host string) []string
}

// Allowed returns the sorted list of permitted command names (for the UI).
func Allowed() []string {
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	// Stable, human-friendly ordering.
	order := []string{"ping", "traceroute", "nslookup", "netstat"}
	filtered := out[:0]
	for _, name := range order {
		if _, ok := registry[name]; ok {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

// resolve validates a request and returns the command name and argv.
func resolve(req protocol.DiagRequest) (name string, argv []string, err error) {
	b, ok := registry[strings.ToLower(strings.TrimSpace(req.Command))]
	if !ok {
		return "", nil, fmt.Errorf("%w: %q", ErrNotWhitelisted, req.Command)
	}
	var host string
	if b.needsHost {
		if len(req.Args) != 1 {
			return "", nil, fmt.Errorf("diag: %s requires exactly one target argument", b.name)
		}
		host = strings.TrimSpace(req.Args[0])
		if err := validateHost(host); err != nil {
			return "", nil, err
		}
	} else if len(req.Args) != 0 {
		return "", nil, fmt.Errorf("diag: %s takes no arguments", b.name)
	}
	return b.name, b.build(host), nil
}

// validateHost enforces the strict host/IP pattern.
func validateHost(host string) error {
	if host == "" {
		return errors.New("diag: empty target")
	}
	if len(host) > 255 {
		return errors.New("diag: target too long")
	}
	if ip := net.ParseIP(host); ip != nil {
		return nil
	}
	if !hostPattern.MatchString(host) {
		return fmt.Errorf("diag: invalid target %q", host)
	}
	return nil
}

// Run executes a whitelisted diagnostic, streaming output to emit as DiagChunks.
// A terminal chunk with EOF=true and the exit code is always emitted. The
// returned error mirrors the failure, if any (also reflected in the EOF chunk).
func Run(ctx context.Context, req protocol.DiagRequest, emit func(protocol.DiagChunk)) error {
	name, argv, err := resolve(req)
	if err != nil {
		emit(protocol.DiagChunk{RequestID: req.RequestID, Stream: "meta", EOF: true, ExitCode: -1, Err: err.Error()})
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, MaxRuntime)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, argv...)
	emit(protocol.DiagChunk{
		RequestID: req.RequestID, Stream: "meta",
		Data: fmt.Sprintf("$ %s %s", name, strings.Join(argv, " ")),
	})

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return finish(req, emit, -1, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return finish(req, emit, -1, err)
	}
	if err := cmd.Start(); err != nil {
		return finish(req, emit, -1, fmt.Errorf("diag: start %s: %w", name, err))
	}

	var (
		wg       sync.WaitGroup
		budgetMu sync.Mutex
		budget   = MaxOutputB
	)
	stream := func(r io.Reader, label string) {
		defer wg.Done()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 4096), chunkScanCap)
		for sc.Scan() {
			line := sc.Text()
			budgetMu.Lock()
			if budget <= 0 {
				budgetMu.Unlock()
				return
			}
			if len(line) > budget {
				line = line[:budget]
			}
			budget -= len(line)
			budgetMu.Unlock()
			emit(protocol.DiagChunk{RequestID: req.RequestID, Stream: label, Data: line})
		}
	}
	wg.Add(2)
	go stream(stdout, "stdout")
	go stream(stderr, "stderr")
	wg.Wait()

	exit := 0
	if err := cmd.Wait(); err != nil {
		exit = -1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		}
		if ctx.Err() == context.DeadlineExceeded {
			return finish(req, emit, exit, fmt.Errorf("diag: %s timed out after %s", name, MaxRuntime))
		}
		return finish(req, emit, exit, nil) // non-zero exit is not an internal error
	}
	return finish(req, emit, exit, nil)
}

func finish(req protocol.DiagRequest, emit func(protocol.DiagChunk), exit int, err error) error {
	ch := protocol.DiagChunk{RequestID: req.RequestID, Stream: "meta", EOF: true, ExitCode: exit}
	if err != nil {
		ch.Err = err.Error()
	}
	emit(ch)
	return err
}
