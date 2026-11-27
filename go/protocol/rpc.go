package protocol

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Request is a JSON-RPC request.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is a JSON-RPC response.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// ParseVersion splits "major.minor".
func ParseVersion(v string) (major, minor int, err error) {
	parts := strings.Split(v, ".")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("version %q is not major.minor", v)
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || major < 0 || minor < 0 {
		return 0, 0, fmt.Errorf("version %q is not major.minor", v)
	}
	return major, minor, nil
}

// ChooseVersion finds the highest major version that both sides speak, and
// the lower of the two minor versions in it. ok is false when no major
// version is shared.
func ChooseVersion(host, plugin []string) (string, bool) {
	best := -1
	bestMinor := 0
	for _, h := range host {
		hm, hn, err := ParseVersion(h)
		if err != nil {
			continue
		}
		for _, p := range plugin {
			pm, pn, err := ParseVersion(p)
			if err != nil || pm != hm {
				continue
			}
			minor := hn
			if pn < minor {
				minor = pn
			}
			if hm > best || (hm == best && minor > bestMinor) {
				best, bestMinor = hm, minor
			}
		}
	}
	if best < 0 {
		return "", false
	}
	return fmt.Sprintf("%d.%d", best, bestMinor), true
}
