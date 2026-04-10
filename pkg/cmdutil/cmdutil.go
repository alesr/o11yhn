// cmdutil provides utility functions for main package
package cmdutil

import (
	"log/slog"
	"os"
	"sync"
)

var (
	baseLogger *slog.Logger
	logOnce    sync.Once
)

func NewLogger(lvl slog.Leveler) *slog.Logger {
	if lvl == nil {
		lvl = slog.LevelInfo
	}

	return slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{
				AddSource: true,
				Level:     lvl,
			},
		),
	)
}

func logger() *slog.Logger {
	logOnce.Do(func() {
		baseLogger = NewLogger(slog.LevelInfo).WithGroup("mainutil")
	})
	return baseLogger
}

func Must[T any](val T, err error) T {
	if err != nil {
		logger().Error("Failed to initialize dependency", "error", err)
		os.Exit(1)
	}
	return val
}
