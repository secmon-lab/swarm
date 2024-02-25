package utils

import (
	"log/slog"
	"sync"
)

var (
	logger      = slog.Default()
	loggerMutex sync.Mutex
)

func Logger() *slog.Logger {
	return logger
}

func SetLogger(l *slog.Logger) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	logger = l
}

func ErrLog(err error) slog.Attr {
	return slog.Any("error", err)
}
