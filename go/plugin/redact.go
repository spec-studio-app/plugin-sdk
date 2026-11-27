package plugin

import (
	"io"
	"strings"
	"sync"
)

// redactor replaces the values of secret settings with [secret].
type redactor struct {
	mu      sync.RWMutex
	secrets []string
}

const secretMark = "[secret]"

func (r *redactor) add(value string) {
	if value == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.secrets {
		if s == value {
			return
		}
	}
	r.secrets = append(r.secrets, value)
	// Replace the longest values first, so a value inside another is not left half.
	for i := len(r.secrets) - 1; i > 0 && len(r.secrets[i]) > len(r.secrets[i-1]); i-- {
		r.secrets[i], r.secrets[i-1] = r.secrets[i-1], r.secrets[i]
	}
}

func (r *redactor) text(s string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, v := range r.secrets {
		if strings.Contains(s, v) {
			s = strings.ReplaceAll(s, v, secretMark)
		}
	}
	return s
}

// redactWriter redacts each write. slog writes one whole record in each
// call, so a secret is never split between two writes.
type redactWriter struct {
	r   *redactor
	mu  sync.Mutex
	out io.Writer
}

func (w *redactWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := io.WriteString(w.out, w.r.text(string(p))); err != nil {
		return 0, err
	}
	return len(p), nil
}
