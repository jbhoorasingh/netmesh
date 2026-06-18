//go:build windows

package dataplane

import "syscall"

// reusePortControl sets SO_REUSEADDR on Windows, which has no SO_REUSEPORT but
// provides comparable shared-binding behaviour for the symmetric prober.
func reusePortControl(network, address string, c syscall.RawConn) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	}); err != nil {
		return err
	}
	return serr
}
