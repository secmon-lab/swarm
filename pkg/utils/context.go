package utils

import (
	"context"
	"log/slog"

	"github.com/m-mizutani/swarm/pkg/domain/types"
)

type ctxRequestIDKey struct{}

// CtxRequestID returns request ID from context. If request ID is not set, return new request ID and context with it
func CtxRequestID(ctx context.Context) (types.RequestID, context.Context) {
	if id, ok := ctx.Value(ctxRequestIDKey{}).(types.RequestID); ok {
		return id, ctx
	}

	newID := types.NewRequestID()
	return newID, context.WithValue(ctx, ctxRequestIDKey{}, newID)
}

type ctxIngestIDKey struct{}

// CtxIngestID returns stream ID from context. If stream ID is not set, return new stream ID and context with it
func CtxIngestID(ctx context.Context) (types.IngestID, context.Context) {
	if id, ok := ctx.Value(ctxIngestIDKey{}).(types.IngestID); ok {
		return id, ctx
	}

	newID := types.NewIngestID()
	return newID, context.WithValue(ctx, ctxIngestIDKey{}, newID)
}

type ctxLoggerKey struct{}

// WithLogger returns a new context with logger
func CtxWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxLoggerKey{}, logger)
}

// CtxLogger returns logger from context. If logger is not set, return default logger
func CtxLogger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxLoggerKey{}).(*slog.Logger); ok {
		return l
	}
	return logger
}
