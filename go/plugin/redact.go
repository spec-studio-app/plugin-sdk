package plugin

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"
)

// redactor replaces the values of secret settings with [secret].
type redactor struct {
	mu      sync.RWMutex
	secrets []string
}

const secretMark = "[secret]"

// add adds a secret value and its escaped forms: slog's text log and %q
// write a value with strconv.Quote, and JSON escapes it in its own way.
func (r *redactor) add(value string) {
	if value == "" {
		return
	}
	quoted := strconv.Quote(value)
	r.addOne(quoted[1 : len(quoted)-1])
	if j, err := json.Marshal(value); err == nil {
		r.addOne(string(j[1 : len(j)-1]))
	}
	r.addOne(value)
}

func (r *redactor) addOne(value string) {
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
