package server

import (
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/utils"
)

// Authorization is a middleware to check the token in Authorization header.
// It returns empty string if the token is not found or invalid.
func Authorization(uc interfaces.UseCase) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				utils.HandleError(r.Context(), "failed to read body", err)
				http.Error(w, "Data read error", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(strings.NewReader(string(body)))

			input := &model.AuthPolicyInput{
				Method: r.Method,
				Path:   r.URL.Path,
				Remote: r.RemoteAddr,
				Query:  r.URL.Query(),
				Header: r.Header,
				Body:   string(body),
			}

			ctx := r.Context()
			if err := uc.Authorize(ctx, input); err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Logging is a middleware to log HTTP access
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		reqID, ctx := utils.CtxRequestID(ctx)
		logger := utils.CtxLogger(ctx)
		logger = logger.With(
			slog.Any("request_id", reqID),
			slog.Group("http",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("remote", r.RemoteAddr),
			),
		)
		ctx = utils.CtxWithLogger(ctx, logger)

		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r.WithContext(ctx))

		logger.Info("http access",
			slog.Int("status", rec.status),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote", r.RemoteAddr),
			slog.Any("query", r.URL.Query()),
			slog.Any("header", r.Header),
			slog.String("user_agent", r.UserAgent()),
		)
	})
}

type ReadMemStatsFn func(m *runtime.MemStats)

func MemoryLimit(limit uint64, read ReadMemStatsFn) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var m runtime.MemStats
			read(&m)
			if m.Sys > limit {
				utils.CtxLogger(r.Context()).Warn("Memory limit exceeded",
					"limit", humanize.Bytes(limit),
					"sys", humanize.Bytes(m.Sys),
				)
				http.Error(w, "Memory limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
