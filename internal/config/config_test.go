package config

import (
	"strings"
	"testing"
)

func TestModeSelection(t *testing.T) {
	tests := []struct {
		name string
		args []string
		mode Mode
		addr string // expected MasterAddr for join mode
	}{
		{"controller", []string{"-master=self"}, ModeController, ""},
		{"holding", nil, ModeAgentHolding, ""},
		{"join-ip", []string{"-master=10.10.10.5"}, ModeAgentJoin, "10.10.10.5:5999"},
		{"join-host-port", []string{"-master=ctrl.local:7000"}, ModeAgentJoin, "ctrl.local:7000"},
		{"join-scheme-stripped", []string{"-master=ws://10.0.0.1"}, ModeAgentJoin, "10.0.0.1:5999"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse("netmesh", tc.args)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if cfg.Mode != tc.mode {
				t.Errorf("Mode = %v, want %v", cfg.Mode, tc.mode)
			}
			if tc.addr != "" && cfg.MasterAddr != tc.addr {
				t.Errorf("MasterAddr = %q, want %q", cfg.MasterAddr, tc.addr)
			}
		})
	}
}

func TestAdminParsing(t *testing.T) {
	cfg, err := Parse("netmesh", []string{"-master=self", "-admin=root:p@ss:word"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.AuthEnabled || cfg.AdminUser != "root" || cfg.AdminPass != "p@ss:word" {
		t.Errorf("admin parsed as user=%q pass=%q enabled=%v", cfg.AdminUser, cfg.AdminPass, cfg.AuthEnabled)
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"admin-on-agent", []string{"-master=10.0.0.1", "-admin=a:b"}, "only valid"},
		{"admin-no-colon", []string{"-master=self", "-admin=nopass"}, "user:pass"},
		{"admin-empty-pass", []string{"-master=self", "-admin=user:"}, "non-empty"},
		{"bad-port", []string{"-master=self", "-port=99999"}, "invalid -port"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse("netmesh", tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want contains %q", err, tc.want)
			}
		})
	}
}

func TestDefaultPort(t *testing.T) {
	cfg, err := Parse("netmesh", []string{"-master=self"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != DefaultPort {
		t.Errorf("Port = %d, want %d", cfg.Port, DefaultPort)
	}
	if cfg.ListenAddr() != ":5999" {
		t.Errorf("ListenAddr = %q, want :5999", cfg.ListenAddr())
	}
}
