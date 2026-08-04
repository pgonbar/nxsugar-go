package nxsugar

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
)

const (
	PanicLevel = "panic"
	FatalLevel = "fatal"
	ErrorLevel = "error"
	WarnLevel  = "warn"
	InfoLevel  = "info"
	DebugLevel = "debug"
)

var (
	loggerMu sync.RWMutex
	logger   = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	customLogger bool
)

func getLogger() *slog.Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	return logger
}

// SetLogger replaces the internal logger.  Call this from the host application
// to share a trace-injecting handler (e.g. logging.Init).  Once called,
// SetLogLevel and SetJSONOutput become no-ops so the injected handler is
// preserved.
func SetLogger(l *slog.Logger) {
	loggerMu.Lock()
	logger = l
	customLogger = true
	loggerMu.Unlock()
}

// SetJSONOutput toggles between JSON and text output.  No-op when a custom
// logger has been injected via SetLogger.
func SetJSONOutput(enabled bool) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	if customLogger {
		return
	}
	var h slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	if enabled {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	logger = slog.New(h)
}

// SetLogLevel sets the minimum log level.  No-op when a custom logger has
// been injected via SetLogger.
func SetLogLevel(level string) {
	loggerMu.Lock()
	defer loggerMu.Unlock()
	if customLogger {
		return
	}

	var lvl slog.Level
	switch strings.ToLower(level) {
	case PanicLevel, FatalLevel:
		lvl = slog.LevelError + 1
	case ErrorLevel:
		lvl = slog.LevelError
	case WarnLevel:
		lvl = slog.LevelWarn
	case InfoLevel:
		lvl = slog.LevelInfo
	default:
		lvl = slog.LevelDebug
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	logger = slog.New(h)
}

// GetLogLevel returns the current minimum log level as a string.
func GetLogLevel() string {
	loggerMu.RLock()
	defer loggerMu.RUnlock()
	switch {
	case logger.Enabled(context.Background(), slog.LevelError+1):
		return PanicLevel
	case logger.Enabled(context.Background(), slog.LevelError):
		return ErrorLevel
	case logger.Enabled(context.Background(), slog.LevelWarn):
		return WarnLevel
	case logger.Enabled(context.Background(), slog.LevelInfo):
		return InfoLevel
	default:
		return DebugLevel
	}
}

// levelFromString maps a nxsugar level string to slog.Level.
func levelFromString(level string) slog.Level {
	switch strings.ToLower(level) {
	case PanicLevel, FatalLevel:
		return slog.LevelError + 1
	case ErrorLevel:
		return slog.LevelError
	case WarnLevel:
		return slog.LevelWarn
	case InfoLevel:
		return slog.LevelInfo
	default:
		return slog.LevelDebug
	}
}

// Log is the package-level entry point kept for backward compatibility.
// Prefer LogCtx when a context.Context is available so that trace_id is
// auto-injected by the handler.
func Log(level string, path string, message string, args ...interface{}) {
	LogWithFields(level, path, nil, message, args...)
}

// LogCtx is the context-aware variant of Log.
func LogCtx(ctx context.Context, level string, path string, message string, args ...interface{}) {
	LogWithFieldsCtx(ctx, level, path, nil, message, args...)
}

// LogWithFields logs a message with structured fields.
func LogWithFields(level string, path string, fields map[string]interface{}, message string, args ...interface{}) {
	LogWithFieldsCtx(context.Background(), level, path, fields, message, args...)
}

// LogWithFieldsCtx is the context-aware variant of LogWithFields.
// Fields are emitted in sorted key order for deterministic output.
func LogWithFieldsCtx(ctx context.Context, level string, path string, fields map[string]interface{}, message string, args ...interface{}) {
	msg := message
	if len(args) > 0 {
		msg = fmt.Sprintf(message, args...)
	}

	attrs := make([]slog.Attr, 0, 1+len(fields))
	attrs = append(attrs, slog.String("path", path))

	if len(fields) > 0 {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			attrs = append(attrs, slog.Any(k, fields[k]))
		}
	}

	getLogger().LogAttrs(ctx, levelFromString(level), msg, attrs...)
}
