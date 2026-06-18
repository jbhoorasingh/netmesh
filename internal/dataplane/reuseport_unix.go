//go:build !windows

package dataplane

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// reusePortControl sets SO_REUSEADDR and SO_REUSEPORT on the raw socket so the
// UDP-symmetric prober (which binds the destination port as its source) can
// coexist with the local responder bound to the same port. It uses
// golang.org/x/sys/unix, which defines SO_REUSEPORT reliably across all unix
// GOOS/GOARCH pairs (unlike the standard syscall package).
func reusePortControl(network, address string, c syscall.RawConn) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); e != nil {
			serr = e
			return
		}
		serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}
	return serr
}
