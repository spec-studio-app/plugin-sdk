package plugin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// ServeStdio serves one connection on in and out: one JSON message per line.
// It returns after a plugin.shutdown, after a shutdown for another reason
// (a signal), or after in closes (with a shutdown of 5 seconds).
func (s *Server) ServeStdio(in io.Reader, out io.Writer) error {
	c := &conn{}
	var writeMu sync.Mutex
	var calls sync.WaitGroup
	stopped := make(chan struct{})
	writeLine := func(resp *protocol.Response) {
		data, err := s.encode(resp)
		if err != nil {
			s.Log.Error("cannot encode an answer", "error", err.Error())
			return
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		out.Write(append(data, '\n'))
	}
	// waitCalls waits for the calls in progress, for at most d.
	waitCalls := func(d time.Duration) {
		done := make(chan struct{})
		go func() {
			calls.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(d):
			s.Log.Warn("calls still in progress at the end of the shutdown")
		}
	}

	lines := make(chan []byte)
	readErr := make(chan error, 1)
	go func() {
		r := bufio.NewReaderSize(in, 64<<10)
		for {
			line, err := r.ReadBytes('\n')
			if len(strings.TrimSpace(string(line))) > 0 {
				select {
				case lines <- line:
				case <-stopped:
					return
				}
			}
			if err != nil {
				select {
				case readErr <- err:
				default:
				}
				return
			}
		}
	}()

	for {
		select {
		case line := <-lines:
			if len(line) > s.limits.MaxMessageBytes {
				writeLine(errorResponse(requestID(line), protocol.RPCError(protocol.RPCInvalidRequest, "The message has %d bytes. The limit is %d bytes.", len(line), s.limits.MaxMessageBytes)))
				continue
			}
			if isShutdown(line) {
				timeout := shutdownTimeout(line)
				start := time.Now()
				resp := s.handle(s.calls, c, line)
				if resp != nil && resp.Error == nil {
					// Answer the shutdown after the other answers, within the timeout.
					waitCalls(timeout - time.Since(start))
					writeLine(resp)
					close(stopped)
					return nil
				}
				if resp != nil {
					writeLine(resp)
				}
				continue
			}
			calls.Add(1)
			go func(line []byte) {
				defer calls.Done()
				if resp := s.handle(s.calls, c, line); resp != nil {
					writeLine(resp)
				}
			}(line)
		case <-s.Done():
			// A shutdown that did not come on this connection: a signal.
			waitCalls(time.Second)
			close(stopped)
			return nil
		case err := <-readErr:
			if !errors.Is(err, io.EOF) {
				s.Log.Error("cannot read stdin", "error", err.Error())
			}
			s.Shutdown(5 * time.Second)
			waitCalls(time.Second)
			close(stopped)
			return nil
		}
	}
}

// requestID reads the id of a request, or gives nil.
func requestID(line []byte) json.RawMessage {
	var head struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(line, &head) != nil {
		return nil
	}
	return head.ID
}

func shutdownTimeout(line []byte) time.Duration {
	var req struct {
		Params protocol.ShutdownParams `json:"params"`
	}
	json.Unmarshal(line, &req)
	if req.Params.TimeoutMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(req.Params.TimeoutMs) * time.Millisecond
}

func isShutdown(line []byte) bool {
	var req struct {
		Method string `json:"method"`
	}
	return json.Unmarshal(line, &req) == nil && req.Method == protocol.MethodShutdown
}

// Handler serves the protocol over HTTP: POST /rpc. With a token, each
// request needs "Authorization: Bearer <token>". A shutdown call runs
// onShutdown after the answer.
func (s *Server) Handler(token string, onShutdown func()) http.Handler {
	var mu sync.Mutex
	sessions := map[string]*conn{}
	newSession := func(c *conn) string {
		id := newID()
		mu.Lock()
		sessions[id] = c
		mu.Unlock()
		return id
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "Use POST.", http.StatusMethodNotAllowed)
			return
		}
		if token != "" && r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "The bearer token is missing or wrong.", http.StatusUnauthorized)
			return
		}
		// A browser page must not reach the plugin: refuse a request from a
		// browser (Origin), a request that is not JSON (a simple cross-origin
		// POST), and, with no token, a Host that is not a loopback name (DNS rebinding).
		if r.Header.Get("Origin") != "" {
			http.Error(w, "The plugin does not take requests from a browser page.", http.StatusForbidden)
			return
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(ct), "application/json") {
			http.Error(w, "Send Content-Type: application/json.", http.StatusUnsupportedMediaType)
			return
		}
		if token == "" && !isLoopbackHost(r.Host) {
			http.Error(w, "With no token, the plugin takes requests only for a loopback host name.", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(s.limits.MaxMessageBytes)))
		var resp *protocol.Response
		if err != nil {
			resp = errorResponse(requestID(body), protocol.RPCError(protocol.RPCInvalidRequest, "The message is larger than %d bytes, or it did not arrive.", s.limits.MaxMessageBytes))
		} else {
			mu.Lock()
			c := sessions[r.Header.Get("Plugin-Session")]
			mu.Unlock()
			if c == nil {
				c = &conn{newSession: newSession}
			}
			resp = s.handle(r.Context(), c, body)
		}
		if resp == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		data, err := s.encode(resp)
		if err != nil {
			http.Error(w, "The plugin cannot encode the answer.", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		if resp.Error == nil && isShutdown(body) && onShutdown != nil {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			go onShutdown()
		}
	})
	return mux
}

// ServeHTTP listens on addr until a shutdown call or until ctx ends.
func (s *Server) ServeHTTP(ctx context.Context, addr, token string) error {
	if token == "" && !isLoopback(addr) {
		return fmt.Errorf("the plugin listens on %s with no token. Set --token (or PLUGIN_TOKEN), or listen on 127.0.0.1", addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.serveListener(ctx, ln, token)
}

func (s *Server) serveListener(ctx context.Context, ln net.Listener, token string) error {
	stop := make(chan struct{})
	var once sync.Once
	srv := &http.Server{Handler: s.Handler(token, func() { once.Do(func() { close(stop) }) }), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	s.Log.Info("listening", "address", ln.Addr().String())
	select {
	case <-stop:
	case <-ctx.Done():
		s.Shutdown(5 * time.Second)
	case err := <-errc:
		return err
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	return nil
}

// isLoopbackHost is true for a Host header that names a loopback address.
func isLoopbackHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Options are the options of Run.
type Options struct {
	// Listen is the HTTP address. Empty: stdio.
	Listen string
	// Token is the HTTP bearer token.
	Token  string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// Run serves the plugin until a shutdown, the end of stdin (stdio), or ctx.
func Run(ctx context.Context, p Plugin, o Options) error {
	if o.Stdin == nil {
		o.Stdin = os.Stdin
	}
	if o.Stdout == nil {
		o.Stdout = os.Stdout
	}
	if o.Stderr == nil {
		o.Stderr = os.Stderr
	}
	if p.Store == nil {
		return errors.New("the plugin has no store")
	}
	for _, st := range p.Settings {
		if !protocol.ValidSettingName(st.Name) {
			return fmt.Errorf("the setting name %q is not valid: use a-z, 0-9 and _, and start with a letter", st.Name)
		}
	}
	s := NewServer(&p, o.Stderr)
	if o.Listen != "" {
		return s.ServeHTTP(ctx, o.Listen, o.Token)
	}
	go func() {
		select {
		case <-ctx.Done():
			s.Shutdown(5 * time.Second)
		case <-s.Done():
		}
	}()
	return s.ServeStdio(o.Stdin, o.Stdout)
}

// Main parses the flags --listen and --token, serves the plugin, and exits.
// Define your own flags before you call Main. SIGINT and SIGTERM shut the
// plugin down.
func Main(p Plugin) {
	listen := flag.String("listen", "", "serve HTTP on this address (for example 127.0.0.1:7070) instead of stdio")
	token := flag.String("token", os.Getenv("PLUGIN_TOKEN"), "the HTTP bearer token (default $PLUGIN_TOKEN)")
	if !flag.Parsed() {
		flag.Parse()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, p, Options{Listen: *listen, Token: *token}); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", p.Info.Name, err)
		os.Exit(1)
	}
	os.Exit(0)
}
