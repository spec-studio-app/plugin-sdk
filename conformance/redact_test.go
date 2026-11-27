package conformance

import (
	"strings"
	"testing"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// The report never shows the value of a secret setting given with --setting.
func TestReportRedactsSecrets(t *testing.T) {
	e := &env{
		target:   Target{Settings: map[string]string{"api_key": `k"ey-123`, "address": "host:1"}},
		describe: protocol.DescribeResult{Settings: []protocol.Setting{{Name: "api_key", Secret: true}, {Name: "address"}}},
	}
	got := e.redact(`got bad_settings: the key k"ey-123 (quoted "k\"ey-123") is wrong for host:1`)
	if strings.Contains(got, "ey-123") {
		t.Fatalf("the report shows the secret: %s", got)
	}
	if !strings.Contains(got, "host:1") {
		t.Fatalf("a setting that is not secret was redacted: %s", got)
	}
	// Before describe, every setting counts as secret.
	e.describe = protocol.DescribeResult{}
	if got := e.redact("host:1"); strings.Contains(got, "host:1") {
		t.Fatalf("an unknown setting was not redacted: %s", got)
	}
}
