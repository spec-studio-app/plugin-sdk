package conformance

import (
	"encoding/json"
	"fmt"

	"path/filepath"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/spec-studio-app/plugin-sdk/go/client"
)

// schemaCheck checks each message against the JSON Schemas in a folder.
type schemaCheck struct {
	schemas map[string]*jsonschema.Schema
	mu      sync.Mutex
	bad     []string
	checked int
}

func loadSchemas(dir string) (*schemaCheck, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no JSON Schema files in %s", dir)
	}
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft2020
	c.AssertFormat = true
	sc := &schemaCheck{schemas: map[string]*jsonschema.Schema{}}
	for _, f := range files {
		abs, _ := filepath.Abs(f)
		s, err := c.Compile(abs)
		if err != nil {
			return nil, fmt.Errorf("the schema %s does not compile: %w", f, err)
		}
		sc.schemas[strings.TrimSuffix(filepath.Base(f), ".json")] = s
	}
	return sc, nil
}

func (sc *schemaCheck) check(dir client.Direction, method string, raw []byte) {
	var msg struct {
		Params json.RawMessage `json:"params"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		sc.addFailure(fmt.Sprintf("%s: the message is not JSON", method))
		return
	}
	var name string
	var part json.RawMessage
	switch {
	case dir == client.Sent:
		name, part = method+".request", msg.Params
		if len(part) == 0 {
			part = json.RawMessage("{}")
		}
	case len(msg.Error) > 0:
		name, part = "error", msg.Error
	default:
		name, part = method+".result", msg.Result
	}
	s := sc.schemas[name]
	if s == nil {
		// A method that the protocol does not have (the unknown-method test).
		return
	}
	var v any
	if err := json.Unmarshal(part, &v); err != nil {
		sc.addFailure(fmt.Sprintf("%s: not JSON", name))
		return
	}
	sc.mu.Lock()
	sc.checked++
	sc.mu.Unlock()
	if err := s.Validate(v); err != nil {
		text := err.Error()
		if len(text) > 300 {
			text = text[:300] + "…"
		}
		sc.addFailure(fmt.Sprintf("%s: %s", name, strings.ReplaceAll(text, "\n", " ")))
	}
}

func (sc *schemaCheck) addFailure(s string) {
	if sc == nil {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.bad) < 20 {
		sc.bad = append(sc.bad, s)
	}
}

func (sc *schemaCheck) failures() []string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.checked == 0 {
		return []string{"no message was checked"}
	}
	return append([]string(nil), sc.bad...)
}

// Checked is the number of messages checked.
func (sc *schemaCheck) Checked() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.checked
}

