// Package conformance runs the protocol tests against any Spec Studio
// backend plugin, over stdio or HTTP. It tests only the capabilities that
// the plugin declares.
package conformance

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/client"
	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// Target is the plugin to test.
type Target struct {
	// Command starts a stdio plugin. Set Command or URL.
	Command []string
	// URL is the address of an HTTP plugin, for example http://127.0.0.1:7070/rpc.
	URL   string
	Token string
	// Settings are the plugin's settings for the handshake.
	Settings map[string]string
	// Schemas is the folder of the JSON Schemas. Empty: no schema check.
	Schemas string
	// PluginStderr also gets the stderr of a stdio plugin. Can be nil.
	PluginStderr io.Writer
	// Run limits the tests to names that match this pattern. Empty: all.
	Run string
}

// Status is the outcome of one test.
type Status string

const (
	Pass Status = "pass"
	Fail Status = "fail"
	Skip Status = "skip"
)

// Result is the outcome of one test.
type Result struct {
	Name     string
	Covers   string
	Status   Status
	Reason   string
	Duration time.Duration
}

// Report is the outcome of a run.
type Report struct {
	Results []Result
}

// Failed is true when a test failed.
func (r Report) Failed() bool {
	for _, x := range r.Results {
		if x.Status == Fail {
			return true
		}
	}
	return false
}

// Count gives the number of results with the status.
func (r Report) Count(s Status) int {
	n := 0
	for _, x := range r.Results {
		if x.Status == s {
			n++
		}
	}
	return n
}

// skipError skips a test with a reason.
type skipError struct{ reason string }

func (s skipError) Error() string { return s.reason }

func skip(format string, args ...any) error { return skipError{fmt.Sprintf(format, args...)} }

type test struct {
	name   string
	covers string
	needs  protocol.Capability
	// since is the protocol version that the test needs. The test runs only
	// when the plugin chose this version or a later one in the handshake.
	since string
	run   func(e *env) error
}

// env is the state of one run.
type env struct {
	ctx      context.Context
	target   Target
	c        *client.Client
	proc     *client.Process
	stderr   *lockedBuffer
	describe protocol.DescribeResult
	info     protocol.HandshakeResult
	ns       string
	schemas  *schemaCheck
	// largest is the size of the largest message so far.
	largest atomic.Int64
	// invalid > 0 while a test sends params that are not valid on purpose.
	invalid atomic.Int32
}

func (e *env) stdio() bool { return len(e.target.Command) > 0 }

// name gives a name in this run's namespace.
func (e *env) name(s string) string { return e.ns + "-" + s }

// expectInvalid runs f with the request schema check off.
func (e *env) expectInvalid(f func() error) error {
	e.invalid.Add(1)
	defer e.invalid.Add(-1)
	return f()
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Run runs the tests and prints each result to out as it ends.
func Run(ctx context.Context, t Target, out io.Writer) (Report, error) {
	if (len(t.Command) == 0) == (t.URL == "") {
		return Report{}, errors.New("give a plugin command or --url, not both")
	}
	var filter *regexp.Regexp
	if t.Run != "" {
		re, err := regexp.Compile(t.Run)
		if err != nil {
			return Report{}, fmt.Errorf("--run: %w", err)
		}
		filter = re
	}
	e := &env{ctx: ctx, target: t, stderr: &lockedBuffer{}, ns: "conformance-" + randomHex(4)}
	if t.Schemas != "" {
		sc, err := loadSchemas(t.Schemas)
		if err != nil {
			return Report{}, err
		}
		e.schemas = sc
	}
	if err := e.connect(); err != nil {
		return Report{}, err
	}
	defer e.close()

	var report Report
	for _, tc := range tests() {
		if filter != nil && !filter.MatchString(tc.name) && !alwaysRun[tc.name] {
			continue
		}
		res := e.runOne(tc)
		res.Reason = e.redact(res.Reason)
		report.Results = append(report.Results, res)
		printResult(out, res)
		if tc.name == "handshake" && res.Status == Fail {
			break
		}
	}
	if e.schemas != nil && (filter == nil || filter.MatchString("schemas")) {
		res := Result{Name: "schemas", Covers: "R13.S2", Status: Pass}
		if bad := e.schemas.failures(); len(bad) > 0 {
			res.Status = Fail
			res.Reason = e.redact(strings.Join(bad, "; "))
		}
		report.Results = append(report.Results, res)
		printResult(out, res)
	}
	fmt.Fprintf(out, "\n%d passed, %d failed, %d skipped\n", report.Count(Pass), report.Count(Fail), report.Count(Skip))
	return report, nil
}

// The tests that set up the connection run whatever the filter.
var alwaysRun = map[string]bool{"describe": true, "handshake": true}

func printResult(out io.Writer, r Result) {
	line := fmt.Sprintf("%-4s  %-28s %-12s", r.Status, r.Name, r.Covers)
	if r.Reason != "" {
		line += "  " + r.Reason
	}
	fmt.Fprintln(out, strings.TrimRight(line, " "))
}

func (e *env) runOne(tc test) (res Result) {
	res = Result{Name: tc.name, Covers: tc.covers}
	start := time.Now()
	defer func() {
		if p := recover(); p != nil {
			res.Status = Fail
			res.Reason = fmt.Sprintf("panic: %v", p)
		}
		res.Duration = time.Since(start)
	}()
	if tc.needs != "" && !e.info.Has(tc.needs) {
		res.Status = Skip
		res.Reason = fmt.Sprintf("the plugin does not declare %s", tc.needs)
		return res
	}
	if tc.since != "" && !protocol.AtLeast(e.info.ProtocolVersion, tc.since) {
		res.Status = Skip
		res.Reason = fmt.Sprintf("the plugin chose protocol %s; the test needs %s", e.info.ProtocolVersion, tc.since)
		return res
	}
	err := tc.run(e)
	var s skipError
	switch {
	case err == nil:
		res.Status = Pass
	case errors.As(err, &s):
		res.Status = Skip
		res.Reason = s.reason
	default:
		res.Status = Fail
		res.Reason = err.Error()
	}
	return res
}

func (e *env) connect() error {
	if e.stdio() {
		var w io.Writer = e.stderr
		if e.target.PluginStderr != nil {
			w = io.MultiWriter(e.stderr, e.target.PluginStderr)
		}
		p, err := client.Start(e.ctx, e.target.Command, w)
		if err != nil {
			return fmt.Errorf("cannot start the plugin: %w", err)
		}
		e.proc = p
		e.c = p.Client
	} else {
		e.c = client.Dial(e.target.URL, e.target.Token)
	}
	e.c.SetObserver(e.observe)
	return nil
}

func (e *env) close() {
	if e.proc != nil {
		e.proc.Close()
		select {
		case <-e.proc.Exited():
		case <-time.After(5 * time.Second):
			e.proc.Kill()
		}
	}
}

func (e *env) observe(dir client.Direction, method string, raw []byte) {
	for {
		cur := e.largest.Load()
		if int64(len(raw)) <= cur || e.largest.CompareAndSwap(cur, int64(len(raw))) {
			break
		}
	}
	if e.schemas == nil {
		return
	}
	if dir == client.Sent && e.invalid.Load() > 0 {
		return
	}
	e.schemas.check(dir, method, raw)
}

// hello is the handshake of the runner.
func (e *env) hello(versions ...string) protocol.HandshakeParams {
	if len(versions) == 0 {
		versions = []string{"1.99"}
	}
	settings := map[string]string{}
	for k, v := range e.target.Settings {
		settings[k] = v
	}
	return protocol.HandshakeParams{Host: protocol.Host{Kind: "test", Version: "conformance"}, ProtocolVersions: versions, Settings: settings}
}

// redact replaces the values of secret settings (all settings when the
// plugin did not describe them) with [secret], in their raw and escaped forms,
// so the report never shows a secret.
func (e *env) redact(text string) string {
	if text == "" {
		return text
	}
	secret := map[string]bool{}
	for _, s := range e.describe.Settings {
		secret[s.Name] = s.Secret
	}
	for name, value := range e.target.Settings {
		if value == "" {
			continue
		}
		if known, ok := secret[name]; ok && !known {
			continue
		}
		q := strconv.Quote(value)
		j, _ := json.Marshal(value)
		for _, v := range []string{q[1 : len(q)-1], string(j[1 : len(j)-1]), value} {
			text = strings.ReplaceAll(text, v, "[secret]")
		}
	}
	return text
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ---- helpers for the tests ----

func expectCode(err error, c protocol.Code) error {
	if err == nil {
		return fmt.Errorf("the call succeeded; want the error %s", c)
	}
	if got := protocol.CodeOf(err); got != c {
		return fmt.Errorf("got %v; want the error %s", err, c)
	}
	return nil
}

func expectRPC(err error, code int) error {
	var pe *protocol.Error
	if !errors.As(err, &pe) {
		return fmt.Errorf("got %v; want the JSON-RPC error %d", err, code)
	}
	if pe.RPC != code {
		return fmt.Errorf("got the error %d (%s); want %d", pe.RPC, pe.Message, code)
	}
	return nil
}

func (e *env) timeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(e.ctx, d)
}
