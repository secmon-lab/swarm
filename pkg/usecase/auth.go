package usecase

import (
	"context"
	"errors"
	"log/slog"

	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/infra/policy"
	"github.com/m-mizutani/swarm/pkg/utils"
)

// UseCase is a usecase of authorization for HTTP access. It uses policy engine to evaluate the access control.
func (x *UseCase) Authorize(ctx context.Context, input *model.AuthPolicyInput) error {
	var output model.AuthPolicyOutput

	var p policy.RegoPrint = func(file string, row int, msg string) error {
		utils.CtxLogger(ctx).Info(msg, slog.Group("rego",
			"file", file,
			"row", row,
		))
		return nil
	}

	if err := x.clients.Policy().Query(ctx, "data.auth", &input, &output, policy.WithRegoPrint(p)); err != nil {
		if !errors.Is(err, types.ErrNoPolicyResult) {
			return goerr.Wrap(err, "failed to evaluate policy").With("input", input)
		}
	}

	utils.CtxLogger(ctx).Debug("authorization result",
		"input", input,
		"output", output,
	)

	if output.Deny {
		return goerr.Wrap(types.ErrUnauthorized, "denied by policy")
	}

	return nil
}
