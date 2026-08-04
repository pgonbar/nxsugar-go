package nxsugar

import (
	"fmt"
	"log/slog"

	nexus "github.com/nayarsystems/nxgo/nxcore"
)

const (
	// nxgo
	ErrParse            = -32700
	ErrInvalidRequest   = -32600
	ErrInternal         = -32603
	ErrInvalidParams    = -32602
	ErrMethodNotFound   = -32601
	ErrTtlExpired       = -32011
	ErrPermissionDenied = -32010
	ErrConnClosed       = -32007
	ErrLockNotOwned     = -32006
	ErrUserExists       = -32005
	ErrInvalidUser      = -32004
	ErrInvalidPipe      = -32003
	ErrInvalidTask      = -32002
	ErrCancel           = -32001
	ErrTimeout          = -32000
	ErrNotSupported     = -32099
	// nxsugar
	ErrTestingMethodNotProvided = -20000
	ErrPactNotDefined           = -20001
)

var ErrStr = map[int]string{
	// nxgo
	ErrParse:            "Parse error",
	ErrInvalidRequest:   "Invalid request",
	ErrMethodNotFound:   "Method not found",
	ErrInvalidParams:    "Invalid params",
	ErrInternal:         "Internal error",
	ErrTimeout:          "Timeout",
	ErrCancel:           "Cancel",
	ErrInvalidTask:      "Invalid task",
	ErrInvalidPipe:      "Invalid pipe",
	ErrInvalidUser:      "Invalid user",
	ErrUserExists:       "User already exists",
	ErrPermissionDenied: "Permission denied",
	ErrTtlExpired:       "TTL expired",
	ErrLockNotOwned:     "Lock not owned",
	ErrConnClosed:       "Connection is closed",
	ErrNotSupported:     "Not supported",
	// nxsugar
	ErrTestingMethodNotProvided: "Testing method not provided",
	ErrPactNotDefined:           "Pact not defined for provided input",
}

type JsonRpcErr struct {
	Cod  int         `json:"code"`
	Mess string      `json:"message"`
	Dat  interface{} `json:"data,omitempty"`
}

func (e *JsonRpcErr) Error() string {
	return fmt.Sprintf("[%d] %s", e.Cod, e.Mess)
}

// LogValue makes *JsonRpcErr serialize as a structured object instead of a
// flat string when passed to a slog handler. The json tags on JsonRpcErr
// already match the desired field names.
func (e *JsonRpcErr) LogValue() slog.Value {
	if e == nil {
		return slog.StringValue("<nil>")
	}
	return slog.AnyValue(*e)
}

func (e *JsonRpcErr) Code() int {
	return e.Cod
}

func (e *JsonRpcErr) Data() interface{} {
	return e.Dat
}

// NewJsonRpcErr creates new JSON-RPC error.
//
// code is the JSON-RPC error code.
// message is optional in case of well known error code (negative values).
// data is an optional extra info object.
func NewJsonRpcErr(code int, message string, data interface{}) *JsonRpcErr {
	if code < 0 {
		if message != "" {
			message = fmt.Sprintf("%s:[%s]", ErrStr[code], message)
		} else {
			message = ErrStr[code]
		}
	}

	return &JsonRpcErr{Cod: code, Mess: message, Dat: data}
}

/*
IsNexusErr checks whether an error is from nexus (it matches the type *nxcore.JsonRpcErr).
*/
func IsNexusErr(err error) bool {
	_, ok := err.(*nexus.JsonRpcErr)
	return ok
}

/*
IsNexusErrCode checks whether an error is from nexus (it matches the type *nxcore.JsonRpcErr) and matches the provided code.
*/
func IsNexusErrCode(err error, code int) bool {
	if nexusErr, ok := err.(*nexus.JsonRpcErr); ok {
		return nexusErr.Cod == code
	}
	return false
}
