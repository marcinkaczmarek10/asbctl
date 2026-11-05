package main

import (
	"runtime/debug"
	"testing"
)

func TestVersionFrom(t *testing.T) {
	tests := []struct {
		name    string
		linked  string
		info    *debug.BuildInfo
		hasInfo bool
		want    string
	}{
		{name: "the linked version wins", linked: "1.4.2", info: &debug.BuildInfo{Main: debug.Module{Version: "v9.9.9"}}, hasInfo: true, want: "1.4.2"},
		{name: "the module version is used when nothing was linked", info: &debug.BuildInfo{Main: debug.Module{Version: "v0.3.1"}}, hasInfo: true, want: "v0.3.1"},
		{name: "an empty module version falls back", info: &debug.BuildInfo{}, hasInfo: true, want: "(devel)"},
		{name: "no build info falls back", want: "(devel)"},
		{name: "the linked version wins without build info", linked: "2.0.0", want: "2.0.0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := versionFrom(tc.linked, tc.info, tc.hasInfo); got != tc.want {
				t.Errorf("versionFrom(%q) = %q, want %q", tc.linked, got, tc.want)
			}
		})
	}
}
