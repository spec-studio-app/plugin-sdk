// Package client is a Go host client for the Spec Studio backend plugin
// protocol. The conformance runner uses it. It starts a stdio plugin or
// connects to an HTTP plugin, and gives one typed call for each method.
package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// Direction is the direction of a message.
type Direction int

const (
	// Sent is a message from the host to the plugin.
	Sent Direction = iota
	// Received is a message from the plugin to the host.
	Received
)

// Observer sees each message, for checks such as the schema check. method
// is the method of the call for both directions.
type Observer func(dir Direction, method string, raw []byte)

type transport interface {
	roundTrip(ctx context.Context, id int64, method string, req []byte) ([]byte, error)
	close() error
}

// Client is one connection to a plugin.
type Client struct {
	t       transport
	nextID  atomic.Int64
	observe Observer
	obsMu   sync.Mutex
	// Info is the handshake result, after Handshake.
	Info protocol.HandshakeResult
}

// SetObserver sets the observer of the messages.
func (c *Client) SetObserver(o Observer) {
	c.obsMu.Lock()
	c.observe = o
	c.obsMu.Unlock()
}

func (c *Client) see(dir Direction, method string, raw []byte) {
	c.obsMu.Lock()
	o := c.observe
	c.obsMu.Unlock()
	if o != nil {
		o(dir, method, raw)
	}
}

// Call sends one request and decodes the result into out. A plugin error
// is a *protocol.Error.
func (c *Client) Call(ctx context.Context, method string, params, out any) error {
	if params == nil {
		params = struct{}{}
	}
	p, err := protocol.MarshalNoEscape(params)
	if err != nil {
		return err
	}
	return c.CallRaw(ctx, method, p, out)
}

// CallRaw sends params as they are.
func (c *Client) CallRaw(ctx context.Context, method string, params json.RawMessage, out any) error {
	id := c.nextID.Add(1)
	req, err := protocol.MarshalNoEscape(protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(strconv.FormatInt(id, 10)), Method: method, Params: params})
	if err != nil {
		return err
	}
	c.see(Sent, method, req)
	raw, err := c.t.roundTrip(ctx, id, method, req)
	if err != nil {
		return err
	}
	c.see(Received, method, raw)
	var resp protocol.Response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("the answer to %s is not JSON-RPC: %w", method, err)
	}
	if resp.Error != nil {
		return resp.Error
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(resp.Result, out); err != nil {
		return fmt.Errorf("the result of %s does not decode: %w", method, err)
	}
	return nil
}

// SendLine sends raw bytes as one line (stdio only), for the tests of bad messages.
func (c *Client) SendLine(ctx context.Context, line []byte) ([]byte, error) {
	st, ok := c.t.(*stdio)
	if !ok {
		return nil, errors.New("SendLine works over stdio only")
	}
	return st.rawLine(ctx, line)
}

// Close closes the connection. It does not ask the plugin to shut down.
func (c *Client) Close() error { return c.t.close() }

// ---- stdio ----

type stdio struct {
	w       io.WriteCloser
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[string]chan []byte
	// nullID gets the answers with id null (a parse error).
	nullID chan []byte
	closed chan struct{}
	err    error
	cmd    *exec.Cmd
	exited chan struct{}
}

// Process is a plugin that the client started.
type Process struct {
	*Client
	cmd    *exec.Cmd
	st     *stdio
	exitMu sync.Mutex
	exit   error
}

// Start starts the plugin command and connects to it over stdio. The
// plugin's stderr goes to stderr.
func Start(ctx context.Context, argv []string, stderr io.Writer) (*Process, error) {
	if len(argv) == 0 {
		return nil, errors.New("no plugin command")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stderr = stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	st := newStdio(out, in)
	st.cmd = cmd
	p := &Process{Client: &Client{t: st}, cmd: cmd, st: st}
	go func() {
		err := cmd.Wait()
		p.exitMu.Lock()
		p.exit = err
		p.exitMu.Unlock()
		close(st.exited)
	}()
	return p, nil
}

// Exited is closed when the process exits.
func (p *Process) Exited() <-chan struct{} { return p.st.exited }

// ExitCode gives the exit code after Exited, or -1.
func (p *Process) ExitCode() int {
	select {
	case <-p.st.exited:
	default:
		return -1
	}
	if p.cmd.ProcessState == nil {
		return -1
	}
	return p.cmd.ProcessState.ExitCode()
}

// Kill kills the process.
func (p *Process) Kill() error { return p.cmd.Process.Kill() }

// Connect makes a client on a reader (the plugin's stdout) and a writer (its stdin).
func Connect(r io.Reader, w io.WriteCloser) *Client {
	return &Client{t: newStdio(r, w)}
}

func newStdio(r io.Reader, w io.WriteCloser) *stdio {
	st := &stdio{w: w, pending: map[string]chan []byte{}, nullID: make(chan []byte, 16), closed: make(chan struct{}), exited: make(chan struct{})}
	go st.read(r)
	return st
}

func (st *stdio) read(r io.Reader) {
	br := bufio.NewReaderSize(r, 64<<10)
	for {
		line, err := br.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var head struct {
				ID json.RawMessage `json:"id"`
			}
			if json.Unmarshal(line, &head) == nil {
				key := string(head.ID)
				if key == "" || key == "null" {
					select {
					case st.nullID <- line:
					default:
					}
				} else {
					st.mu.Lock()
					ch := st.pending[key]
					delete(st.pending, key)
					st.mu.Unlock()
					if ch != nil {
						ch <- line
					}
				}
			}
			// A line that is not JSON is not a protocol message: ignore it.
		}
		if err != nil {
			st.mu.Lock()
			if errors.Is(err, io.EOF) {
				st.err = protocol.NewError(protocol.Unavailable, "The plugin closed its stdout.")
			} else {
				st.err = protocol.NewError(protocol.Unavailable, "The plugin's stdout failed: %s", err.Error())
			}
			st.mu.Unlock()
			close(st.closed)
			return
		}
	}
}

func (st *stdio) send(line []byte) error {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	_, err := st.w.Write(append(line, '\n'))
	if err != nil {
		return protocol.NewError(protocol.Unavailable, "The plugin's stdin is closed: %s", err.Error())
	}
	return nil
}

func (st *stdio) roundTrip(ctx context.Context, id int64, method string, req []byte) ([]byte, error) {
	key := strconv.FormatInt(id, 10)
	ch := make(chan []byte, 1)
	st.mu.Lock()
	if st.err != nil {
		err := st.err
		st.mu.Unlock()
		return nil, err
	}
	st.pending[key] = ch
	st.mu.Unlock()
	if err := st.send(req); err != nil {
		st.mu.Lock()
		delete(st.pending, key)
		st.mu.Unlock()
		return nil, err
	}
	select {
	case raw := <-ch:
		return raw, nil
	case <-st.closed:
		select {
		case raw := <-ch:
			return raw, nil
		default:
		}
		st.mu.Lock()
		err := st.err
		st.mu.Unlock()
		return nil, err
	case <-ctx.Done():
		st.mu.Lock()
		delete(st.pending, key)
		st.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (st *stdio) rawLine(ctx context.Context, line []byte) ([]byte, error) {
	if err := st.send(line); err != nil {
		return nil, err
	}
	select {
	case raw := <-st.nullID:
		return raw, nil
	case <-st.closed:
		return nil, st.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (st *stdio) close() error { return st.w.Close() }

// ---- HTTP ----

type httpTransport struct {
	url     string
	token   string
	client  *http.Client
	mu      sync.Mutex
	session string
}

// Dial makes a client for a plugin that serves HTTP at url (for example
// http://127.0.0.1:7070/rpc). token can be empty.
func Dial(url, token string) *Client {
	return &Client{t: &httpTransport{url: url, token: token, client: &http.Client{}}}
}

func (h *httpTransport) roundTrip(ctx context.Context, id int64, method string, req []byte) ([]byte, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(req))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	if h.token != "" {
		r.Header.Set("Authorization", "Bearer "+h.token)
	}
	h.mu.Lock()
	if h.session != "" {
		r.Header.Set("Plugin-Session", h.session)
	}
	h.mu.Unlock()
	resp, err := h.client.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, protocol.NewError(protocol.Unavailable, "The plugin at %s does not answer: %s", h.url, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, protocol.NewError(protocol.Unavailable, "The answer from %s did not arrive: %s", h.url, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{Status: resp.StatusCode, Body: string(body)}
	}
	if method == protocol.MethodHandshake {
		var hs struct {
			Result *protocol.HandshakeResult `json:"result"`
		}
		if json.Unmarshal(body, &hs) == nil && hs.Result != nil && hs.Result.Session != "" {
			h.mu.Lock()
			h.session = hs.Result.Session
			h.mu.Unlock()
		}
	}
	return body, nil
}

func (h *httpTransport) close() error { return nil }

// HTTPError is an HTTP answer that is not 200.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// ---- typed calls ----

// Describe calls plugin.describe.
func (c *Client) Describe(ctx context.Context) (protocol.DescribeResult, error) {
	var r protocol.DescribeResult
	return r, c.Call(ctx, protocol.MethodDescribe, nil, &r)
}

// Handshake calls plugin.handshake and keeps the result in Info.
func (c *Client) Handshake(ctx context.Context, p protocol.HandshakeParams) (protocol.HandshakeResult, error) {
	if p.Settings == nil {
		p.Settings = map[string]string{}
	}
	var r protocol.HandshakeResult
	if err := c.Call(ctx, protocol.MethodHandshake, p, &r); err != nil {
		return r, err
	}
	c.Info = r
	return r, nil
}

// Ping calls plugin.ping.
func (c *Client) Ping(ctx context.Context) (protocol.PingResult, error) {
	var r protocol.PingResult
	return r, c.Call(ctx, protocol.MethodPing, nil, &r)
}

// Shutdown calls plugin.shutdown.
func (c *Client) Shutdown(ctx context.Context, timeout time.Duration) error {
	return c.Call(ctx, protocol.MethodShutdown, protocol.ShutdownParams{TimeoutMs: int(timeout.Milliseconds())}, nil)
}

// Get calls state.get.
func (c *Client) Get(ctx context.Context, collection, key string) (protocol.StateGetResult, error) {
	var r protocol.StateGetResult
	return r, c.Call(ctx, protocol.MethodStateGet, protocol.StateGetParams{Collection: collection, Key: key}, &r)
}

// Put calls state.put.
func (c *Client) Put(ctx context.Context, p protocol.StatePutParams) (protocol.StatePutResult, error) {
	var r protocol.StatePutResult
	return r, c.Call(ctx, protocol.MethodStatePut, p, &r)
}

// Delete calls state.delete.
func (c *Client) Delete(ctx context.Context, collection, key, expected string) error {
	return c.Call(ctx, protocol.MethodStateDelete, protocol.StateDeleteParams{Collection: collection, Key: key, ExpectedVersion: expected}, nil)
}

// List calls state.list.
func (c *Client) List(ctx context.Context, p protocol.StateListParams) (protocol.StateListResult, error) {
	var r protocol.StateListResult
	return r, c.Call(ctx, protocol.MethodStateList, p, &r)
}

// Commit calls state.commit (protocol 1.1). It writes the document and
// appends the events in one atomic step. A nil p.Events sends an empty list.
// On a version mismatch the error is a *protocol.Error with the code
// conflict and the stored version in Data.Version.
func (c *Client) Commit(ctx context.Context, p protocol.StateCommitParams) (protocol.StateCommitResult, error) {
	if p.Events == nil {
		p.Events = []protocol.NewEvent{}
	}
	var r protocol.StateCommitResult
	return r, c.Call(ctx, protocol.MethodStateCommit, p, &r)
}

// Append calls events.append.
func (c *Client) Append(ctx context.Context, stream string, events ...protocol.NewEvent) (protocol.EventsAppendResult, error) {
	if events == nil {
		events = []protocol.NewEvent{}
	}
	var r protocol.EventsAppendResult
	return r, c.Call(ctx, protocol.MethodEventsAppend, protocol.EventsAppendParams{Stream: stream, Events: events}, &r)
}

// Read calls events.read.
func (c *Client) Read(ctx context.Context, p protocol.EventsReadParams) (protocol.EventsReadResult, error) {
	var r protocol.EventsReadResult
	return r, c.Call(ctx, protocol.MethodEventsRead, p, &r)
}

// ReadAll reads every event of a stream after a cursor, page by page.
func (c *Client) ReadAll(ctx context.Context, p protocol.EventsReadParams) ([]protocol.Event, error) {
	var out []protocol.Event
	for {
		r, err := c.Read(ctx, p)
		if err != nil {
			return nil, err
		}
		out = append(out, r.Events...)
		if !r.More || len(r.Events) == 0 {
			return out, nil
		}
		p.After = r.Cursor
	}
}

// DeleteStream calls events.delete.
func (c *Client) DeleteStream(ctx context.Context, stream string) (bool, error) {
	var r protocol.EventsDeleteResult
	return r.Deleted, c.Call(ctx, protocol.MethodEventsDelete, protocol.EventsDeleteParams{Stream: stream}, &r)
}

// PutBlob puts a blob in chunks of the plugin's chunk limit and returns its SHA-256.
func (c *Client) PutBlob(ctx context.Context, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	return sha, c.PutBlobAs(ctx, sha, data)
}

// PutBlobAs puts a blob with the hash given (a wrong hash is for tests).
func (c *Client) PutBlobAs(ctx context.Context, sha string, data []byte) error {
	var begin protocol.BlobsBeginResult
	if err := c.Call(ctx, protocol.MethodBlobsBegin, protocol.BlobsBeginParams{SHA256: sha, Size: int64(len(data))}, &begin); err != nil {
		return err
	}
	if begin.Exists {
		return nil
	}
	chunk := c.chunk()
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		var w protocol.BlobsWriteResult
		if err := c.Call(ctx, protocol.MethodBlobsWrite, protocol.BlobsWriteParams{UploadID: begin.UploadID, Offset: int64(off), Data: data[off:end]}, &w); err != nil {
			return err
		}
	}
	return c.Call(ctx, protocol.MethodBlobsCommit, protocol.BlobsCommitParams{UploadID: begin.UploadID}, nil)
}

func (c *Client) chunk() int {
	if c.Info.Limits.MaxChunkBytes > 0 {
		return c.Info.Limits.MaxChunkBytes
	}
	return protocol.DefaultMaxChunkBytes
}

// GetBlob gets a whole blob, chunk by chunk.
func (c *Client) GetBlob(ctx context.Context, sha string) ([]byte, error) {
	var out []byte
	var off int64
	for {
		var r protocol.BlobsGetResult
		if err := c.Call(ctx, protocol.MethodBlobsGet, protocol.BlobsGetParams{SHA256: sha, Offset: off, Length: c.chunk()}, &r); err != nil {
			return nil, err
		}
		out = append(out, r.Data...)
		off += int64(len(r.Data))
		if off >= r.Size || len(r.Data) == 0 {
			return out, nil
		}
	}
}

// HasBlobs calls blobs.has.
func (c *Client) HasBlobs(ctx context.Context, shas ...string) ([]string, error) {
	if shas == nil {
		shas = []string{}
	}
	var r protocol.BlobsHasResult
	return r.Present, c.Call(ctx, protocol.MethodBlobsHas, protocol.BlobsHasParams{SHA256s: shas}, &r)
}

// DeleteBlob calls blobs.delete.
func (c *Client) DeleteBlob(ctx context.Context, sha string) (bool, error) {
	var r protocol.BlobsDeleteResult
	return r.Deleted, c.Call(ctx, protocol.MethodBlobsDelete, protocol.BlobsDeleteParams{SHA256: sha}, &r)
}

// Wait calls watch.wait.
func (c *Client) Wait(ctx context.Context, p protocol.WatchWaitParams) (protocol.WatchWaitResult, error) {
	var r protocol.WatchWaitResult
	return r, c.Call(ctx, protocol.MethodWatchWait, p, &r)
}

// ContinueStream calls the test hook test.continueStream.
func (c *Client) ContinueStream(ctx context.Context, stream string) (string, error) {
	var r protocol.ContinueStreamResult
	return r.Run, c.Call(ctx, protocol.MethodContinueStream, protocol.ContinueStreamParams{Stream: stream}, &r)
}
