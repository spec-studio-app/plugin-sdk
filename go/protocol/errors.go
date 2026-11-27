package protocol

import (
	"errors"
	"fmt"
	"regexp"
)

// Code is the word of a plugin error (data.code).
type Code string

const (
	NotFound    Code = "not_found"
	Conflict    Code = "conflict"
	Closing     Code = "closing"
	Dropped     Code = "dropped"
	Refused     Code = "refused"
	Unavailable Code = "unavailable"
	BadSettings Code = "bad_settings"
)

// Codes are all the plugin error codes, in order.
var Codes = []Code{NotFound, Conflict, Closing, Dropped, Refused, Unavailable, BadSettings}

// The JSON-RPC codes.
const (
	RPCParseError     = -32700
	RPCInvalidRequest = -32600
	RPCMethodNotFound = -32601
	RPCInvalidParams  = -32602
	RPCInternal       = -32603
)

var rpcCodes = map[Code]int{
	NotFound:    -32001,
	Conflict:    -32002,
	Closing:     -32003,
	Dropped:     -32004,
	Refused:     -32005,
	Unavailable: -32006,
	BadSettings: -32007,
}

// RPCCode gives the JSON-RPC code of a plugin error code.
func RPCCode(c Code) int { return rpcCodes[c] }

// ErrorData is the data of a plugin error.
type ErrorData struct {
	Code Code `json:"code"`
	// Version is the stored version, for a conflict.
	Version *string `json:"version,omitempty"`
	// Reason gives more detail for some errors: ReasonHandshakeRequired.
	Reason string `json:"reason,omitempty"`
}

// ReasonHandshakeRequired is the reason of a refused call before the
// handshake. Over HTTP, a host that gets it sends the handshake again (the
// plugin restarted and lost the session) and retries the call.
const ReasonHandshakeRequired = "handshake_required"

// Error is a JSON-RPC error. A plugin error has Data with its Code.
type Error struct {
	RPC     int        `json:"code"`
	Message string     `json:"message"`
	Data    *ErrorData `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e.Data != nil {
		return fmt.Sprintf("%s: %s", e.Data.Code, e.Message)
	}
	return fmt.Sprintf("rpc error %d: %s", e.RPC, e.Message)
}

// Code gives the plugin error code, or "" for a JSON-RPC error with no data.
func (e *Error) Code() Code {
	if e.Data == nil {
		return ""
	}
	return e.Data.Code
}

// NewError makes a plugin error.
func NewError(c Code, format string, args ...any) *Error {
	return &Error{RPC: RPCCode(c), Message: fmt.Sprintf(format, args...), Data: &ErrorData{Code: c}}
}

// ConflictError makes a conflict with the stored version (nil: no document).
func ConflictError(stored *string) *Error {
	e := NewError(Conflict, "The stored version is not the expected version. Read the document again.")
	if stored != nil {
		e.Data.Version = stored
	} else {
		e.Data.Version = nil
	}
	return e
}

// MarshalJSON keeps "version": null in a conflict with no document.
func (d ErrorData) MarshalJSON() ([]byte, error) {
	m := map[string]any{"code": d.Code}
	if d.Code == Conflict {
		m["version"] = d.Version
	} else if d.Version != nil {
		m["version"] = *d.Version
	}
	if d.Reason != "" {
		m["reason"] = d.Reason
	}
	return MarshalNoEscape(m)
}

// RPCError makes a JSON-RPC error with no plugin code.
func RPCError(code int, format string, args ...any) *Error {
	return &Error{RPC: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf gives the plugin error code of err, or "".
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code()
	}
	return ""
}

// Is is true when err is a plugin error with the code.
func Is(err error, c Code) bool { return CodeOf(err) == c }

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,199}$`)

// ValidName is true for a valid name of a collection, a key or a stream. A
// name has no "/" and starts with a letter or a digit, so it is never "." or
// ".." and it is always one path segment.
func ValidName(s string) bool {
	return nameRE.MatchString(s)
}

var settingRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ValidSettingName is true for a valid setting name: a-z, 0-9 and _, from a letter.
func ValidSettingName(s string) bool { return settingRE.MatchString(s) }

// CheckName gives a refused error for a name that is not valid.
func CheckName(what, s string) error {
	if ValidName(s) {
		return nil
	}
	return NewError(Refused, "The %s name %q is not valid. Use 1 to 200 characters from A-Z a-z 0-9 . _ : @ -, and start with a letter or a digit.", what, s)
}

var shaRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ValidSHA256 is true for a SHA-256 in lowercase hex.
func ValidSHA256(s string) bool { return shaRE.MatchString(s) }
