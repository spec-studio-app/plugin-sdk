package protocol_test

import (
	"os"
	"strings"
	"testing"
)

// The methods of the relay's src/store/types.ts (relay 0.5.0).
var relayMethods = map[string][]string{
	"SpecStore":        {"sync", "state", "refresh", "gone", "send", "ack", "read", "list"},
	"DeskStore":        {"report", "get", "send", "ack", "sync", "read", "list"},
	"RelayConfigStore": {"verifyAgent", "paired", "pair", "pairCodeValid", "setPairCode", "unpair", "setRepos", "repos", "getClient", "registerClient", "markSignedIn", "issueTokens", "rotateRefresh", "token", "revoke"},
	"RelayStore":       {"start", "close"},
}

// R13.S1: PROTOCOL.md maps each store method to protocol calls.
func TestProtocolMapsEveryRelayMethod(t *testing.T) {
	data, err := os.ReadFile("../../PROTOCOL.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)
	for iface, methods := range relayMethods {
		for _, m := range methods {
			row := "| `" + iface + "." + m + "` |"
			if !strings.Contains(doc, row) {
				t.Errorf("PROTOCOL.md has no mapping row for %s.%s", iface, m)
			}
		}
	}
}

// Each method of the protocol has a section and a schema pair.
func TestEveryMethodHasASchema(t *testing.T) {
	methods := []string{
		"plugin.describe", "plugin.handshake", "plugin.ping", "plugin.shutdown",
		"state.get", "state.put", "state.delete", "state.list",
		"events.append", "events.read", "events.delete",
		"blobs.begin", "blobs.write", "blobs.commit", "blobs.get", "blobs.has", "blobs.delete",
		"watch.wait", "test.continueStream",
	}
	doc, _ := os.ReadFile("../../PROTOCOL.md")
	for _, m := range methods {
		for _, part := range []string{"request", "result"} {
			if _, err := os.Stat("../../schema/" + m + "." + part + ".json"); err != nil {
				t.Errorf("no schema for %s %s", m, part)
			}
		}
		if !strings.Contains(string(doc), "`"+m) {
			t.Errorf("PROTOCOL.md does not describe %s", m)
		}
		for _, part := range []string{"request", "result"} {
			if !strings.Contains(string(doc), "(schema/"+m+"."+part+".json)") {
				t.Errorf("PROTOCOL.md does not link the %s schema of %s", part, m)
			}
		}
	}
}
