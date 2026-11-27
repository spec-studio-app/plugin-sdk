package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// Server answers the calls of one plugin. One Server serves one stdio
// connection or many HTTP sessions.
type Server struct {
	p      *Plugin
	limits protocol.Limits
	red    *redactor
	// Log is the plugin's log on stderr, with the secrets redacted.
	Log *slog.Logger

	mu       sync.Mutex
	opened   bool
	settings map[string]string
	closing  bool
	inflight sync.WaitGroup
	done     chan struct{}

	// stop ends each watch at shutdown.
	stop       context.Context
	cancelStop context.CancelFunc
	// calls is the context of the stdio calls. The shutdown cancels it at its timeout.
	calls       context.Context
	cancelCalls context.CancelFunc

	uploads *uploads
}

// conn is the state of one connection (stdio) or one HTTP session.
type conn struct {
	mu         sync.Mutex
	handshaken bool
	// newSession is set for HTTP: the handshake gives a session.
	newSession func(*conn) string
}

// NewServer makes a server for a plugin. The log goes to stderr.
func NewServer(p *Plugin, stderr io.Writer) *Server {
	red := &redactor{}
	log := slog.New(slog.NewTextHandler(&redactWriter{r: red, out: stderr}, nil))
	stop, cancel := context.WithCancel(context.Background())
	calls, cancelCalls := context.WithCancel(context.Background())
	s := &Server{
		calls:       calls,
		cancelCalls: cancelCalls,
		p:           p,
		limits:      p.limits(),
		red:         red,
		Log:         log,
		done:        make(chan struct{}),
		stop:        stop,
		cancelStop:  cancel,
	}
	s.uploads = newUploads(s)
	return s
}

// Done is closed when the shutdown is complete.
func (s *Server) Done() <-chan struct{} { return s.done }

// handle answers one message. It returns nil for a message that needs no answer.
func (s *Server) handle(ctx context.Context, c *conn, raw []byte) *protocol.Response {
	var req protocol.Request
	if err := json.Unmarshal(raw, &req); err != nil {
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) || !json.Valid(raw) {
			return errorResponse(nil, protocol.RPCError(protocol.RPCParseError, "The message is not valid JSON."))
		}
		return errorResponse(nil, protocol.RPCError(protocol.RPCInvalidRequest, "The message is not a JSON-RPC request."))
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		return errorResponse(req.ID, protocol.RPCError(protocol.RPCInvalidRequest, "The message is not a JSON-RPC 2.0 request."))
	}
	if len(req.ID) == 0 {
		// A notification: the protocol has none, so it gets no answer.
		return nil
	}
	result, err := s.call(ctx, c, req.Method, req.Params)
	if err != nil {
		return errorResponse(req.ID, s.toError(req.Method, err))
	}
	data, err := marshal(result)
	if err != nil {
		return errorResponse(req.ID, s.toError(req.Method, err))
	}
	return &protocol.Response{JSONRPC: "2.0", ID: req.ID, Result: data}
}

func errorResponse(id json.RawMessage, e *protocol.Error) *protocol.Response {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return &protocol.Response{JSONRPC: "2.0", ID: id, Error: e}
}

// toError makes a protocol error with no secret in its message.
func (s *Server) toError(method string, err error) *protocol.Error {
	var e *protocol.Error
	if errors.As(err, &e) {
		out := *e
		out.Message = s.red.text(e.Message)
		return &out
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return protocol.NewError(protocol.Unavailable, "The call to %s did not end in time.", method)
	}
	text := s.red.text(err.Error())
	s.Log.Error("call failed", "method", method, "error", text)
	return protocol.RPCError(protocol.RPCInternal, "The plugin failed in %s: %s", method, text)
}

var knownMethods = map[string]bool{
	protocol.MethodDescribe: true, protocol.MethodHandshake: true, protocol.MethodPing: true, protocol.MethodShutdown: true,
	protocol.MethodStateGet: true, protocol.MethodStatePut: true, protocol.MethodStateDelete: true, protocol.MethodStateList: true,
	protocol.MethodEventsAppend: true, protocol.MethodEventsRead: true, protocol.MethodEventsDelete: true,
	protocol.MethodBlobsBegin: true, protocol.MethodBlobsWrite: true, protocol.MethodBlobsCommit: true,
	protocol.MethodBlobsGet: true, protocol.MethodBlobsHas: true, protocol.MethodBlobsDelete: true,
	protocol.MethodWatchWait: true, protocol.MethodContinueStream: true,
}

// errShutdown asks the transport to stop after it sends the answer.
type shutdownResult struct{ protocol.Empty }

func (s *Server) call(ctx context.Context, c *conn, method string, params json.RawMessage) (any, error) {
	switch method {
	case protocol.MethodDescribe:
		return s.p.describe(), nil
	case protocol.MethodHandshake:
		return s.handshake(ctx, c, params)
	}
	if !knownMethods[method] {
		return nil, protocol.RPCError(protocol.RPCMethodNotFound, "The method %q does not exist.", method)
	}
	c.mu.Lock()
	ready := c.handshaken
	c.mu.Unlock()
	if !ready {
		e := protocol.NewError(protocol.Refused, "Send plugin.handshake first.")
		e.Data.Reason = protocol.ReasonHandshakeRequired
		return nil, e
	}
	if method == protocol.MethodShutdown {
		var p protocol.ShutdownParams
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.TimeoutMs <= 0 {
			p.TimeoutMs = 5000
		}
		s.Shutdown(time.Duration(p.TimeoutMs) * time.Millisecond)
		return shutdownResult{}, nil
	}
	if !s.enter() {
		return nil, protocol.NewError(protocol.Unavailable, "The plugin shuts down.")
	}
	defer s.inflight.Done()
	if capability, ok := protocol.CapabilityOf(method); ok && !s.has(capability) {
		return nil, protocol.NewError(protocol.Refused, "The plugin %s does not give the %s capability.", s.p.Info.Name, capability)
	}
	return s.dispatch(ctx, method, params)
}

func (s *Server) has(c protocol.Capability) bool {
	for _, x := range Capabilities(s.p.Store) {
		if x == c {
			return true
		}
	}
	return false
}

// enter counts a call in progress. It is false during a shutdown.
func (s *Server) enter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.inflight.Add(1)
	return true
}

func decode(params json.RawMessage, v any) error {
	if len(params) == 0 || string(params) == "null" {
		params = json.RawMessage("{}")
	}
	if err := json.Unmarshal(params, v); err != nil {
		return protocol.RPCError(protocol.RPCInvalidParams, "The params are not valid: %s", err.Error())
	}
	return nil
}

func (s *Server) handshake(ctx context.Context, c *conn, params json.RawMessage) (any, error) {
	var p protocol.HandshakeParams
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	// Redact the secrets first, so no later log line or error can show one.
	for _, st := range s.p.Settings {
		if st.Secret {
			s.red.add(p.Settings[st.Name])
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handshaken {
		return nil, protocol.NewError(protocol.Refused, "The handshake is done already on this connection.")
	}
	if s.isClosing() {
		return nil, protocol.NewError(protocol.Unavailable, "The plugin shuts down.")
	}
	if p.Host.Kind != "app" && p.Host.Kind != "relay" && p.Host.Kind != "test" {
		return nil, protocol.RPCError(protocol.RPCInvalidParams, "The host kind %q is not app, relay or test.", p.Host.Kind)
	}
	chosen, ok := protocol.ChooseVersion(p.ProtocolVersions, s.p.versions())
	if !ok {
		return nil, protocol.NewError(protocol.Refused,
			"The host and the plugin share no major protocol version. The host speaks %s; the plugin %s speaks %s. Update the host or the plugin.",
			strings.Join(p.ProtocolVersions, ", "), s.p.Info.Name, strings.Join(s.p.versions(), ", "))
	}
	if err := s.checkSettings(p.Settings); err != nil {
		return nil, err
	}
	if err := s.open(ctx, p.Host, p.Settings); err != nil {
		return nil, err
	}
	c.handshaken = true
	s.Log.Info("handshake", "host", p.Host.Kind, "hostVersion", p.Host.Version, "protocol", chosen)
	result := protocol.HandshakeResult{
		Plugin:          s.p.Info,
		ProtocolVersion: chosen,
		Capabilities:    Capabilities(s.p.Store),
		TestHooks:       TestHooks(s.p.Store),
		Durable:         isDurable(s.p.Store),
		Limits:          s.limits,
	}
	if c.newSession != nil {
		result.Session = c.newSession(c)
	}
	return result, nil
}

func (s *Server) checkSettings(values map[string]string) error {
	declared := map[string]protocol.Setting{}
	for _, st := range s.p.Settings {
		declared[st.Name] = st
	}
	var unknown []string
	for name := range values {
		if _, ok := declared[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return protocol.NewError(protocol.BadSettings, "The plugin %s has no setting %s. Remove it.", s.p.Info.Name, strings.Join(unknown, ", "))
	}
	for _, st := range s.p.Settings {
		if st.Required && values[st.Name] == "" {
			return protocol.NewError(protocol.BadSettings, "The setting %s is required: %s", st.Name, st.Description)
		}
	}
	return nil
}

func (s *Server) open(ctx context.Context, host protocol.Host, values map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened {
		if !sameSettings(s.settings, values) {
			return protocol.NewError(protocol.BadSettings, "The plugin runs with other settings already. Restart it to change them.")
		}
		return nil
	}
	copied := map[string]string{}
	for k, v := range values {
		copied[k] = v
	}
	if err := s.p.Store.Open(ctx, OpenOptions{Settings: copied, Host: host, Log: s.Log}); err != nil {
		var e *protocol.Error
		if errors.As(err, &e) {
			return err
		}
		return protocol.NewError(protocol.Unavailable, "The plugin cannot open its storage: %s", err.Error())
	}
	s.opened = true
	s.settings = copied
	return nil
}

func sameSettings(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Shutdown refuses new calls, ends the watches, waits for the calls in
// progress for at most timeout, and closes the store. A second call waits
// for the first.
func (s *Server) Shutdown(timeout time.Duration) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		<-s.done
		return
	}
	s.closing = true
	opened := s.opened
	s.mu.Unlock()

	s.cancelStop()
	waited := make(chan struct{})
	go func() {
		s.inflight.Wait()
		close(waited)
	}()
	timer := time.NewTimer(timeout)
	select {
	case <-waited:
	case <-timer.C:
		s.Log.Warn("shutdown timeout: calls still in progress", "timeoutMs", timeout.Milliseconds())
		// Cancel the calls that did not end, so they stop at the store.
		s.cancelCalls()
	}
	timer.Stop()
	s.uploads.closeAll()
	if opened {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if err := s.p.Store.Close(ctx); err != nil {
			s.Log.Error("close failed", "error", s.red.text(err.Error()))
		}
		cancel()
	}
	s.Log.Info("shutdown")
	close(s.done)
}

// encode marshals an answer. An answer larger than maxMessageBytes becomes
// a refused error, so no message on the transport is over the limit.
func (s *Server) encode(resp *protocol.Response) ([]byte, error) {
	data, err := marshal(resp)
	if err != nil {
		return nil, err
	}
	if len(data) <= s.limits.MaxMessageBytes {
		return data, nil
	}
	return json.Marshal(errorResponse(resp.ID, protocol.NewError(protocol.Refused,
		"The answer has %d bytes, more than the limit of %d bytes. Ask for a smaller page or a smaller chunk.", len(data), s.limits.MaxMessageBytes)))
}

// marshal writes JSON with no HTML escapes, so a value takes the same bytes
// in an answer as in the request that stored it.
func marshal(v any) ([]byte, error) { return protocol.MarshalNoEscape(v) }

// The room that an answer needs around a stored value: the envelope, the
// version, the time and the attributes are counted apart.
const envelopeBytes = 4096

// checkSize refuses a value that a later answer could not carry.
func (s *Server) checkSize(what string, n int) error {
	room := envelopeBytes
	if room > s.limits.MaxMessageBytes/4 {
		room = s.limits.MaxMessageBytes / 4
	}
	if limit := s.limits.MaxMessageBytes - room; n > limit {
		return protocol.NewError(protocol.Refused, "The %s has %d bytes. The limit is %d bytes, so that a read can return it. Store large data as a blob.", what, n, limit)
	}
	return nil
}

func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)
}
