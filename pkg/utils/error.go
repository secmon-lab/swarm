package utils

import (
	"context"
	"fmt"

	"github.com/getsentry/sentry-go"
	"github.com/getsentry/sentry-go/attribute"
	"github.com/m-mizutani/goerr/v2"
)

func HandleError(ctx context.Context, msg string, err error) {
	// Sending error to Sentry
	hub := sentry.CurrentHub().Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		for k, v := range goerr.Values(err) {
			scope.SetAttributes(attribute.String(fmt.Sprintf("%v", k), fmt.Sprintf("%v", v)))
		}
	})
	evID := hub.CaptureException(err)

	CtxLogger(ctx).Error(msg, ErrLog(err), "sentry.EventID", evID)
}
