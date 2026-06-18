// Package auth implements the Controller's role-based access model.
//
//   - Open Controller (no -admin): every request is treated as fully
//     privileged. Anyone can view telemetry, start tests, and open the
//     diagnostics console.
//   - Secured Controller (-admin=user:pass): unauthenticated requests get
//     read-only access (topology, grid, graphs). Privileged actions — starting
//     tests, changing frequencies, the diagnostics console — require valid
//     credentials, enforced by RequireWrite.
//
// Credentials are verified with constant-time comparison. Basic Auth is the
// default scheme; a JWT issuer can be layered on top of Authenticated without
// changing call sites.
package auth

import (
	"crypto/subtle"
	"net/http"

	"netmesh/internal/logging"
)

// Authenticator enforces the access model described above.
type Authenticator struct {
	enabled bool
	user    string
	pass    string
	log     *logging.Logger
}

// New builds an Authenticator. When enabled is false the Controller is open and
// every request is considered privileged.
func New(enabled bool, user, pass string, log *logging.Logger) *Authenticator {
	return &Authenticator{enabled: enabled, user: user, pass: pass, log: log}
}

// Enabled reports whether credential enforcement is active.
func (a *Authenticator) Enabled() bool { return a.enabled }

// Authenticated reports whether the request is privileged. An open Controller
// grants this to everyone; a secured Controller requires matching Basic Auth
// credentials.
func (a *Authenticator) Authenticated(r *http.Request) bool {
	if !a.enabled {
		return true
	}
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(a.user)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(a.pass)) == 1
	return userOK && passOK
}

// RequireWrite wraps a handler so that only privileged requests reach it.
// Unauthenticated requests receive a 401 with a Basic challenge.
func (a *Authenticator) RequireWrite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.Authenticated(r) {
			next.ServeHTTP(w, r)
			return
		}
		a.log.Emit(logging.Event{
			Type:   logging.AuthRejected,
			Detail: r.Method + " " + r.URL.Path,
			Fields: map[string]any{"remote": r.RemoteAddr},
		})
		w.Header().Set("WWW-Authenticate", `Basic realm="NetMesh", charset="UTF-8"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
	})
}

// RequireWriteFunc is the http.HandlerFunc convenience form of RequireWrite.
func (a *Authenticator) RequireWriteFunc(next http.HandlerFunc) http.Handler {
	return a.RequireWrite(next)
}
