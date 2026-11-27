package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/spec-studio-app/plugin-sdk/go/protocol"
)

// UploadIdle is how long an upload stays with no call.
var UploadIdle = 10 * time.Minute

type upload struct {
	mu       sync.Mutex
	sha      string
	size     int64
	received int64
	file     *os.File
	hash     hash.Hash
	lastUsed time.Time
}

// uploads keeps the chunks of each blob upload in a temporary file.
type uploads struct {
	s    *Server
	mu   sync.Mutex
	byID map[string]*upload
	once sync.Once
}

func newUploads(s *Server) *uploads {
	return &uploads{s: s, byID: map[string]*upload{}}
}

func (u *uploads) sweep() {
	u.mu.Lock()
	defer u.mu.Unlock()
	now := time.Now()
	for id, up := range u.byID {
		up.mu.Lock()
		idle := now.Sub(up.lastUsed) > UploadIdle
		up.mu.Unlock()
		if idle {
			up.discard()
			delete(u.byID, id)
		}
	}
}

func (u *uploads) startSweeper() {
	u.once.Do(func() {
		go func() {
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for {
				select {
				case <-u.s.stop.Done():
					return
				case <-t.C:
					u.sweep()
				}
			}
		}()
	})
}

func (up *upload) discard() {
	if up.file != nil {
		name := up.file.Name()
		up.file.Close()
		os.Remove(name)
		up.file = nil
	}
}

func (u *uploads) begin(ctx context.Context, p protocol.BlobsBeginParams) (protocol.BlobsBeginResult, error) {
	if err := checkSHA(p.SHA256); err != nil {
		return protocol.BlobsBeginResult{}, err
	}
	if p.Size < 0 {
		return protocol.BlobsBeginResult{}, protocol.RPCError(protocol.RPCInvalidParams, "The size is negative.")
	}
	present, err := u.s.p.Store.(BlobStore).HasBlobs(ctx, []string{p.SHA256})
	if err != nil {
		return protocol.BlobsBeginResult{}, err
	}
	if len(present) > 0 {
		return protocol.BlobsBeginResult{Exists: true}, nil
	}
	f, err := os.CreateTemp("", "plugin-upload-*")
	if err != nil {
		return protocol.BlobsBeginResult{}, protocol.NewError(protocol.Unavailable, "The plugin cannot make a temporary file for the upload: %s", err.Error())
	}
	u.startSweeper()
	id := newID()
	u.mu.Lock()
	u.byID[id] = &upload{sha: p.SHA256, size: p.Size, file: f, hash: sha256.New(), lastUsed: time.Now()}
	u.mu.Unlock()
	return protocol.BlobsBeginResult{Exists: false, UploadID: id}, nil
}

func (u *uploads) get(id string) (*upload, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	up, ok := u.byID[id]
	if !ok {
		return nil, protocol.NewError(protocol.NotFound, "The upload %q does not exist, or it ended. Call blobs.begin again.", id)
	}
	return up, nil
}

func (u *uploads) drop(id string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if up, ok := u.byID[id]; ok {
		up.discard()
		delete(u.byID, id)
	}
}

func (u *uploads) write(p protocol.BlobsWriteParams) (protocol.BlobsWriteResult, error) {
	up, err := u.get(p.UploadID)
	if err != nil {
		return protocol.BlobsWriteResult{}, err
	}
	if len(p.Data) > u.s.limits.MaxChunkBytes {
		return protocol.BlobsWriteResult{}, protocol.NewError(protocol.Refused, "The chunk has %d bytes. The limit is %d bytes.", len(p.Data), u.s.limits.MaxChunkBytes)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.file == nil {
		return protocol.BlobsWriteResult{}, protocol.NewError(protocol.NotFound, "The upload %q ended. Call blobs.begin again.", p.UploadID)
	}
	if p.Offset != up.received {
		return protocol.BlobsWriteResult{}, protocol.NewError(protocol.Refused, "The offset is %d. The plugin has %d bytes: send the chunk at that offset.", p.Offset, up.received)
	}
	if up.received+int64(len(p.Data)) > up.size {
		return protocol.BlobsWriteResult{}, protocol.NewError(protocol.Refused, "The chunk goes past the size of the blob (%d bytes).", up.size)
	}
	if _, err := up.file.Write(p.Data); err != nil {
		return protocol.BlobsWriteResult{}, protocol.NewError(protocol.Unavailable, "The plugin cannot write the upload: %s", err.Error())
	}
	up.hash.Write(p.Data)
	up.received += int64(len(p.Data))
	up.lastUsed = time.Now()
	return protocol.BlobsWriteResult{Received: up.received}, nil
}

func (u *uploads) commit(ctx context.Context, p protocol.BlobsCommitParams) (protocol.BlobsCommitResult, error) {
	up, err := u.get(p.UploadID)
	if err != nil {
		return protocol.BlobsCommitResult{}, err
	}
	defer u.drop(p.UploadID)
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.file == nil {
		return protocol.BlobsCommitResult{}, protocol.NewError(protocol.NotFound, "The upload %q ended. Call blobs.begin again.", p.UploadID)
	}
	if up.received != up.size {
		return protocol.BlobsCommitResult{}, protocol.NewError(protocol.Refused, "The upload has %d bytes, not %d. The plugin kept nothing.", up.received, up.size)
	}
	got := hex.EncodeToString(up.hash.Sum(nil))
	if got != up.sha {
		return protocol.BlobsCommitResult{}, protocol.NewError(protocol.Refused, "The SHA-256 of the bytes is %s, not %s. The plugin kept nothing.", got, up.sha)
	}
	if _, err := up.file.Seek(0, io.SeekStart); err != nil {
		return protocol.BlobsCommitResult{}, protocol.NewError(protocol.Unavailable, "The plugin cannot read the upload: %s", err.Error())
	}
	if err := u.s.p.Store.(BlobStore).PutBlob(ctx, up.sha, up.size, up.file); err != nil {
		return protocol.BlobsCommitResult{}, err
	}
	return protocol.BlobsCommitResult{SHA256: up.sha, Size: up.size}, nil
}

func (u *uploads) closeAll() {
	u.mu.Lock()
	defer u.mu.Unlock()
	for id, up := range u.byID {
		up.mu.Lock()
		up.discard()
		up.mu.Unlock()
		delete(u.byID, id)
	}
}
