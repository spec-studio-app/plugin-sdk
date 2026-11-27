package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spec-studio-app/plugin-sdk/conformance"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "conformance-test")
	if err != nil {
		panic(err)
	}
	bin = dir
	for name, pkg := range map[string]string{
		"memory":             "../examples/memory",
		"fixture":            "./internal/fixture",
		"plugin-conformance": "./cmd/plugin-conformance",
	} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(dir, name), ".")
		cmd.Dir = pkg
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			panic("cannot build " + name + ": " + err.Error())
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func run(t *testing.T, target conformance.Target) (conformance.Report, string) {
	t.Helper()
	if target.Schemas == "" {
		target.Schemas = "../schema"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var out bytes.Buffer
	report, err := conformance.Run(ctx, target, &out)
	if err != nil {
		t.Fatal(err)
	}
	return report, out.String()
}

func status(r conformance.Report, name string) conformance.Status {
	for _, x := range r.Results {
		if x.Name == name {
			return x.Status
		}
	}
	return ""
}

// R10.S1, R12.S1, R8.S1, R9.S4
func TestMemoryOverStdio(t *testing.T) {
	data := filepath.Join(t.TempDir(), "memory.json")
	report, out := run(t, conformance.Target{Command: []string{filepath.Join(bin, "memory"), "--data", data}})
	if report.Failed() {
		t.Fatalf("the memory plugin failed:\n%s", out)
	}
	for _, name := range append([]string{"restart", "events-continued-run", "blobs-large", "watch-change", "schemas", "shutdown"}, commitTests...) {
		if s := status(report, name); s != conformance.Pass {
			t.Errorf("%s: %s, want pass\n%s", name, s, out)
		}
	}
}

// The tests of state.commit (protocol 1.1).
var commitTests = []string{"commit-writes-both", "commit-stale-version", "commit-no-events", "commit-create", "commit-race"}

// R10.S1, R8.S2
func TestMemoryOverHTTP(t *testing.T) {
	addr := freeAddr(t)
	cmd := exec.Command(filepath.Join(bin, "memory"), "--listen", addr, "--token", "tok")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	waitListen(t, addr)
	report, out := run(t, conformance.Target{URL: "http://" + addr + "/rpc", Token: "tok"})
	if report.Failed() {
		t.Fatalf("the memory plugin failed over HTTP:\n%s", out)
	}
	for _, name := range commitTests {
		if s := status(report, name); s != conformance.Pass {
			t.Errorf("%s: %s, want pass\n%s", name, s, out)
		}
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the HTTP plugin exited with %v after the shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the HTTP plugin did not exit after the shutdown")
	}
}

// R10.S2, R12.S3
func TestStateOnlyPlugin(t *testing.T) {
	report, out := run(t, conformance.Target{Command: []string{filepath.Join(bin, "fixture")}})
	if report.Failed() {
		t.Fatalf("the state-only fixture failed:\n%s", out)
	}
	for _, name := range []string{"events-order", "blobs-roundtrip", "watch-change"} {
		if s := status(report, name); s != conformance.Skip {
			t.Errorf("%s: %s, want skip", name, s)
		}
	}
	for _, name := range []string{"handshake", "state-race", "capability-refused", "shutdown"} {
		if s := status(report, name); s != conformance.Pass {
			t.Errorf("%s: %s, want pass", name, s)
		}
	}
	if !strings.Contains(out, "skip  events-order") {
		t.Errorf("the output does not print the skipped test:\n%s", out)
	}
}

// A plugin with watch and no events passes: the wait on a stream needs events.
func TestWatchWithNoEvents(t *testing.T) {
	report, out := run(t, conformance.Target{Command: []string{filepath.Join(bin, "fixture"), "--watch"}})
	if report.Failed() {
		t.Fatalf("the watch fixture failed:\n%s", out)
	}
	if s := status(report, "watch-stream"); s != conformance.Skip {
		t.Fatalf("watch-stream: %s, want skip\n%s", s, out)
	}
	for _, name := range []string{"watch-change", "watch-timeout", "watch-already-newer"} {
		if s := status(report, name); s != conformance.Pass {
			t.Errorf("%s: %s, want pass", name, s)
		}
	}
}

// R12.S4
func TestContinuedRunNeedsTheHook(t *testing.T) {
	report, out := run(t, conformance.Target{Command: []string{filepath.Join(bin, "fixture"), "--events"}})
	if report.Failed() {
		t.Fatalf("the events fixture failed:\n%s", out)
	}
	if s := status(report, "events-continued-run"); s != conformance.Skip {
		t.Fatalf("events-continued-run: %s, want skip\n%s", s, out)
	}
	if s := status(report, "events-order"); s != conformance.Pass {
		t.Fatalf("events-order: %s, want pass", s)
	}
	// A plugin with events and no state.commit speaks 1.0: the commit tests skip.
	for _, name := range commitTests {
		if s := status(report, name); s != conformance.Skip {
			t.Errorf("%s: %s, want skip\n%s", name, s, out)
		}
	}
	if !strings.Contains(out, "the plugin chose protocol 1.0") {
		t.Errorf("the output does not say why the commit tests skip:\n%s", out)
	}
}

// A state.commit that appends the events before the version check fails the
// tests of a conflict and of a race.
func TestBrokenCommitFails(t *testing.T) {
	report, out := run(t, conformance.Target{Command: []string{filepath.Join(bin, "fixture"), "--broken-commit"}})
	for _, name := range []string{"commit-stale-version", "commit-race"} {
		if s := status(report, name); s != conformance.Fail {
			t.Errorf("%s: %s, want fail\n%s", name, s, out)
		}
	}
	for _, name := range []string{"commit-writes-both", "commit-no-events", "commit-create"} {
		if s := status(report, name); s != conformance.Pass {
			t.Errorf("%s: %s, want pass\n%s", name, s, out)
		}
	}
}

// R12.S2
func TestBrokenCompareAndSwapFails(t *testing.T) {
	cmd := exec.Command(filepath.Join(bin, "plugin-conformance"), "--", filepath.Join(bin, "fixture"), "--broken-cas")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("exit %v, want code 1\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "fail  state-stale-version") || !strings.Contains(out.String(), "conflict") {
		t.Fatalf("the output does not show the failed test and its reason:\n%s", out.String())
	}
}

// R2.S3
func TestSecretCanary(t *testing.T) {
	report, out := run(t, conformance.Target{Command: []string{filepath.Join(bin, "fixture"), "--secret"}, Settings: map[string]string{"api_key": "real-key-123"}})
	if s := status(report, "secret-canary"); s != conformance.Pass {
		t.Fatalf("secret-canary: %s\n%s", s, out)
	}
	if report.Failed() {
		t.Fatalf("the secret fixture failed:\n%s", out)
	}
}

func TestUsageErrors(t *testing.T) {
	cmd := exec.Command(filepath.Join(bin, "plugin-conformance"))
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 {
		t.Fatalf("exit %v, want code 2", err)
	}
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitListen(t *testing.T, addr string) {
	for i := 0; i < 100; i++ {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nothing listens on %s", addr)
}
