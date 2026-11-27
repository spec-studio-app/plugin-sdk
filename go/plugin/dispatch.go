package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

func (s *Server) dispatch(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	store := s.p.Store
	switch method {
	case protocol.MethodPing:
		return protocol.PingResult{Status: "ok", Time: time.Now().UTC()}, nil

	case protocol.MethodStateGet:
		var p protocol.StateGetParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := checkDoc(p.Collection, p.Key); err != nil {
			return nil, err
		}
		return store.Get(ctx, p)

	case protocol.MethodStatePut:
		var p protocol.StatePutParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := checkDoc(p.Collection, p.Key); err != nil {
			return nil, err
		}
		if len(p.Value) == 0 {
			return nil, protocol.RPCError(protocol.RPCInvalidParams, "The params have no value.")
		}
		if err := checkAttributes(p.Attributes); err != nil {
			return nil, err
		}
		if p.Attributes == nil {
			p.Attributes = protocol.Attributes{}
		}
		if p.ExpectedVersion != nil && *p.ExpectedVersion == "" {
			return nil, protocol.RPCError(protocol.RPCInvalidParams, "The expected version is empty. Use null to make a new document.")
		}
		return store.Put(ctx, p)

	case protocol.MethodStateDelete:
		var p protocol.StateDeleteParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := checkDoc(p.Collection, p.Key); err != nil {
			return nil, err
		}
		if p.ExpectedVersion == "" {
			return nil, protocol.RPCError(protocol.RPCInvalidParams, "The params have no expected version.")
		}
		return store.Delete(ctx, p)

	case protocol.MethodStateList:
		var p protocol.StateListParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := protocol.CheckName("collection", p.Collection); err != nil {
			return nil, err
		}
		if err := checkAttributes(p.Filter); err != nil {
			return nil, err
		}
		p.PageSize = s.pageSize(p.PageSize)
		res, err := store.List(ctx, p)
		if err == nil && res.Items == nil {
			res.Items = []protocol.Document{}
		}
		return res, err

	case protocol.MethodEventsAppend:
		var p protocol.EventsAppendParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := protocol.CheckName("stream", p.Stream); err != nil {
			return nil, err
		}
		if len(p.Events) == 0 {
			return nil, protocol.NewError(protocol.Refused, "The event list is empty. Send at least one event.")
		}
		for i, e := range p.Events {
			if e.Type == "" || len(e.Type) > 200 {
				return nil, protocol.RPCError(protocol.RPCInvalidParams, "Event %d has no type, or a type longer than 200 characters.", i)
			}
			if len(e.Data) == 0 {
				p.Events[i].Data = json.RawMessage("null")
			}
		}
		return s.p.Store.(EventStore).Append(ctx, p)

	case protocol.MethodEventsRead:
		var p protocol.EventsReadParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := protocol.CheckName("stream", p.Stream); err != nil {
			return nil, err
		}
		p.Limit = s.pageSize(p.Limit)
		res, err := s.p.Store.(EventStore).Read(ctx, p)
		if err == nil && res.Events == nil {
			res.Events = []protocol.Event{}
		}
		return res, err

	case protocol.MethodEventsDelete:
		var p protocol.EventsDeleteParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := protocol.CheckName("stream", p.Stream); err != nil {
			return nil, err
		}
		return s.p.Store.(EventStore).DeleteStream(ctx, p)

	case protocol.MethodBlobsBegin:
		var p protocol.BlobsBeginParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		return s.uploads.begin(ctx, p)

	case protocol.MethodBlobsWrite:
		var p protocol.BlobsWriteParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		return s.uploads.write(p)

	case protocol.MethodBlobsCommit:
		var p protocol.BlobsCommitParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		return s.uploads.commit(ctx, p)

	case protocol.MethodBlobsGet:
		var p protocol.BlobsGetParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := checkSHA(p.SHA256); err != nil {
			return nil, err
		}
		if p.Offset < 0 {
			return nil, protocol.RPCError(protocol.RPCInvalidParams, "The offset is negative.")
		}
		if p.Length <= 0 || p.Length > s.limits.MaxChunkBytes {
			p.Length = s.limits.MaxChunkBytes
		}
		data, size, err := s.p.Store.(BlobStore).GetBlob(ctx, p.SHA256, p.Offset, p.Length)
		if err != nil {
			return nil, err
		}
		if data == nil {
			data = []byte{}
		}
		return protocol.BlobsGetResult{Data: data, Offset: p.Offset, Size: size}, nil

	case protocol.MethodBlobsHas:
		var p protocol.BlobsHasParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if len(p.SHA256s) > 1000 {
			return nil, protocol.NewError(protocol.Refused, "Ask for at most 1000 hashes in one call.")
		}
		for _, h := range p.SHA256s {
			if err := checkSHA(h); err != nil {
				return nil, err
			}
		}
		present, err := s.p.Store.(BlobStore).HasBlobs(ctx, p.SHA256s)
		if err != nil {
			return nil, err
		}
		if present == nil {
			present = []string{}
		}
		return protocol.BlobsHasResult{Present: present}, nil

	case protocol.MethodBlobsDelete:
		var p protocol.BlobsDeleteParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := checkSHA(p.SHA256); err != nil {
			return nil, err
		}
		deleted, err := s.p.Store.(BlobStore).DeleteBlob(ctx, p.SHA256)
		return protocol.BlobsDeleteResult{Deleted: deleted}, err

	case protocol.MethodWatchWait:
		var p protocol.WatchWaitParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		return s.wait(ctx, p)

	case protocol.MethodContinueStream:
		hook, ok := s.p.Store.(StreamContinuer)
		if !ok {
			return nil, protocol.NewError(protocol.Refused, "The plugin %s does not declare the continueStream test hook.", s.p.Info.Name)
		}
		var p protocol.ContinueStreamParams
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if err := protocol.CheckName("stream", p.Stream); err != nil {
			return nil, err
		}
		return hook.ContinueStream(ctx, p)
	}
	return nil, protocol.RPCError(protocol.RPCMethodNotFound, "The method %q does not exist.", method)
}

func (s *Server) wait(ctx context.Context, p protocol.WatchWaitParams) (any, error) {
	if p.IsStream() {
		if p.Collection != "" || p.Key != "" {
			return nil, protocol.RPCError(protocol.RPCInvalidParams, "Wait on a document or on a stream, not both.")
		}
		if err := protocol.CheckName("stream", p.Stream); err != nil {
			return nil, err
		}
	} else if err := checkDoc(p.Collection, p.Key); err != nil {
		return nil, err
	}
	timeout := p.TimeoutMs
	if timeout < 0 {
		timeout = 0
	}
	if timeout > s.limits.MaxWaitMs {
		timeout = s.limits.MaxWaitMs
	}
	p.TimeoutMs = timeout
	wctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()
	// End the wait at a shutdown.
	stop := context.AfterFunc(s.stop, cancel)
	defer stop()
	res, err := s.p.Store.(Watcher).Wait(wctx, p)
	if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && ctx.Err() == nil {
		return protocol.WatchWaitResult{Changed: false}, nil
	}
	return res, err
}

func (s *Server) pageSize(n int) int {
	if n <= 0 {
		return protocol.DefaultPageSize
	}
	if n > s.limits.MaxPageSize {
		return s.limits.MaxPageSize
	}
	return n
}

func checkDoc(collection, key string) error {
	if err := protocol.CheckName("collection", collection); err != nil {
		return err
	}
	return protocol.CheckName("key", key)
}

func checkSHA(h string) error {
	if protocol.ValidSHA256(h) {
		return nil
	}
	return protocol.NewError(protocol.Refused, "The hash %q is not a SHA-256 in lowercase hex.", h)
}

func checkAttributes(a protocol.Attributes) error {
	for k, v := range a {
		switch v.(type) {
		case nil, string, float64, bool, json.Number:
		default:
			return protocol.RPCError(protocol.RPCInvalidParams, "The attribute %q is not a string, a number, a boolean or null.", k)
		}
	}
	return nil
}
