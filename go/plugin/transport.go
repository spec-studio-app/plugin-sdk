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
// It returns after a plugin.shutdown, or after in closes (with a shutdown of
// 5 seconds).
func (s *Server) ServeStdio(in io.Reader, out io.Writer) error {
	c := &conn{}
	var writeMu sync.Mutex
	var calls sync.WaitGroup
	stopped := make(chan struct{})
	writeLine := func(resp *protocol.Response) {
		data, err := json.Marshal(resp)
		if err != nil {
			s.Log.Error("cannot encode an answer", "error", err.Error())
			return
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		out.Write(append(data, '\n'))
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
				readErr <- err
				return
			}
		}
	}()

	for {
		select {
		case line := <-lines:
			if len(line) > s.limits.MaxMessageBytes {
				writeLine(errorResponse(nil, protocol.RPCError(protocol.RPCInvalidRequest, "The message has %d bytes. The limit is %d bytes.", len(line), s.limits.MaxMessageBytes)))
				continue
			}
			if isShutdown(line) {
				// Answer the shutdown after every other answer.
				resp := s.handle(context.Background(), c, line)
				calls.Wait()
				if resp != nil {
					writeLine(resp)
				}
				if resp != nil && resp.Error == nil {
					close(stopped)
					return nil
				}
				continue
			}
			calls.Add(1)
			go func(line []byte) {
				defer calls.Done()
				if resp := s.handle(context.Background(), c, line); resp != nil {
					writeLine(resp)
				}
			}(line)
		case err := <-readErr:
			if !errors.Is(err, io.EOF) {
				s.Log.Error("cannot read stdin", "error", err.Error())
			}
			s.Shutdown(5 * time.Second)
			calls.Wait()
			close(stopped)
			return nil
		}
	}
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
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(s.limits.MaxMessageBytes)))
		var resp *protocol.Response
		if err != nil {
			resp = errorResponse(nil, protocol.RPCError(protocol.RPCInvalidRequest, "The message is larger than %d bytes, or it did not arrive.", s.limits.MaxMessageBytes))
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
		data, _ := json.Marshal(resp)
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
