package usecase_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/m-mizutani/gt"
	"github.com/secmon-lab/swarm/pkg/domain/model"
	"github.com/secmon-lab/swarm/pkg/domain/types"
	"github.com/secmon-lab/swarm/pkg/infra"
	"github.com/secmon-lab/swarm/pkg/infra/cs"
	"github.com/secmon-lab/swarm/pkg/infra/policy"
	"github.com/secmon-lab/swarm/pkg/usecase"
)

func TestObjectToSources(t *testing.T) {
	const srcRule = `
src contains {"parser": "json", "schema": "access_log"} if {
	input.cs.bucket == "my-bucket"
}
`
	testCases := map[string]struct {
		policy  string
		sources int
		err     error
	}{
		"src without ignore": {
			policy:  "package event\n" + srcRule,
			sources: 1,
		},
		"src with ignore false": {
			policy:  "package event\nignore := false\n" + srcRule,
			sources: 1,
		},
		"ignore true without src": {
			policy:  "package event\nignore := true\n",
			sources: 0,
		},
		"ignore true with src": {
			policy: "package event\nignore := true\n" + srcRule,
			err:    types.ErrInvalidPolicyResult,
		},
		"neither src nor ignore": {
			policy: "package event\n" + `src contains {"parser": "json", "schema": "access_log"} if { input.cs.bucket == "other-bucket" }`,
			err:    types.ErrNoPolicyResult,
		},
		"event package not defined": {
			policy: "package other\nx := 1\n",
			err:    types.ErrNoPolicyResult,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			p := gt.R1(policy.New(policy.WithPolicyData("event.rego", tc.policy))).NoError(t)
			uc := usecase.New(infra.New(infra.WithPolicy(p)))

			sources, err := uc.ObjectToSources(context.Background(), model.Object{
				CS: &model.CloudStorageObject{Bucket: "my-bucket", Name: "logs/obj.json"},
			})

			if tc.err != nil {
				gt.True(t, errors.Is(err, tc.err))
				return
			}
			gt.NoError(t, err)
			gt.A(t, sources).Length(tc.sources)
		})
	}
}

func TestLoadDataByObjectIgnoredObject(t *testing.T) {
	var opened int
	csClient := &cs.Mock{
		MockAttrs: func(ctx context.Context, obj model.CloudStorageObject) (*storage.ObjectAttrs, error) {
			return &storage.ObjectAttrs{Bucket: string(obj.Bucket), Name: string(obj.Name)}, nil
		},
		MockOpen: func(ctx context.Context, obj model.CloudStorageObject) (io.ReadCloser, error) {
			opened++
			return nil, errObjectNotFound
		},
	}
	p := gt.R1(policy.New(policy.WithDir("testdata/policy"))).NoError(t)
	uc := usecase.New(infra.New(infra.WithCloudStorage(csClient), infra.WithPolicy(p)))

	// The event rule in testdata/policy ignores objects under "ignored/" in "cloudtrail-logs".
	gt.NoError(t, uc.LoadDataByObject(context.Background(), "gs://cloudtrail-logs/ignored/obj.txt"))
	gt.Equal(t, opened, 0)
}
