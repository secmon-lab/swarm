package utils

import (
	"context"
	"fmt"

	"github.com/getsentry/sentry-go"
	"github.com/m-mizutani/goerr/v2"
)

func HandleError(ctx context.Context, msg string, err error) {
	// Sending error to Sentry
	hub := sentry.CurrentHub().Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		for k, v := range goerr.Values(err) {
			scope.SetExtra(fmt.Sprintf("%v", k), v)
		}
	})
	evID := hub.CaptureException(err)

	CtxLogger(ctx).Error(msg, ErrLog(err), "sentry.EventID", evID)
}
