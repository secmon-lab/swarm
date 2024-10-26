package utils

import (
	"log/slog"
	"sync"

	"github.com/secmon-lab/swarm/pkg/domain/types"
)

var (
	logger = slog.Default().With(slog.Group("ctx",
		slog.String("app_version", types.AppVersion),
	))
	loggerMutex sync.Mutex
)

func Logger() *slog.Logger {
	return logger
}

func SetLogger(l *slog.Logger) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	logger = l.With(slog.Group("ctx",
		slog.String("app_version", types.AppVersion),
	))
}

func ErrLog(err error) slog.Attr {
	return slog.Any("error", err)
}
