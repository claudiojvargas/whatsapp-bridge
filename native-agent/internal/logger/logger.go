package logger

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

func New(level string) (*slog.Logger, error) {
	var slogLevel slog.Level

	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug

	case "info":
		slogLevel = slog.LevelInfo

	case "warn":
		slogLevel = slog.LevelWarn

	case "error":
		slogLevel = slog.LevelError

	default:
		return nil, fmt.Errorf(
			"invalid log level: %s",
			level,
		)
	}

	handler := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{
			Level: slogLevel,
		},
	)

	return slog.New(handler), nil
}
