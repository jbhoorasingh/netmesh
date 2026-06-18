// Package web embeds the NetMesh UI assets into the binary so the single
// netmesh executable serves its own UI with no external files.
//
// Today this is the placeholder shell (index.html). Once the Claude Design
// project is synced (NetMesh.dc.html), the compiled assets drop in here and are
// served unchanged.
package web

import (
	"embed"
	"io/fs"
)

//go:embed index.html app.js
var assets embed.FS

// FS returns the embedded UI filesystem rooted at the asset directory.
func FS() fs.FS { return assets }
