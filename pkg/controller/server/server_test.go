package server_test

import (
	"bytes"
	"context"
	_ "embed"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/controller/server"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/usecase"
)

//go:embed testdata/http/pubsub.json
var pubsubBody []byte

func TestEventRequest(t *testing.T) {
	testCases := map[string]struct {
		method     string
		path       string
		body       []byte
		expect     int
		calledLoad int
		calledE2S  int
	}{
		"valid pubsub": {
			method:     http.MethodPost,
			path:       "/event/pubsub/cs",
			body:       pubsubBody,
			expect:     http.StatusOK,
			calledLoad: 1,
			calledE2S:  1,
		},
		"invalid pubsub": {
			method:     http.MethodPost,
			path:       "/event/pubsub/cs",
			body:       []byte("invalid"),
			expect:     http.StatusBadRequest,
			calledLoad: 0,
			calledE2S:  0,
		},
		"invalid path": {
			method:     http.MethodPost,
			path:       "/invalid",
			body:       []byte("invalid"),
			expect:     http.StatusNotFound,
			calledLoad: 0,
			calledE2S:  0,
		},
		"invalid method": {
			method:     http.MethodGet,
			path:       "/event/pubsub/cs",
			body:       pubsubBody,
			expect:     http.StatusMethodNotAllowed,
			calledLoad: 0,
			calledE2S:  0,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			var calledLoad, calledE2S int
			mock := &usecase.Mock{
				MockLoadData: func(ctx context.Context, req []*model.LoadRequest) error {
					gt.A(t, req).Must().Length(1)
					gt.Equal(t, req[0].Source.Parser, types.JSONParser)
					gt.Equal(t, req[0].Source.Schema, "cloudtrail")
					gt.Equal(t, req[0].Source.Compress, types.NoCompress)
					gt.Equal(t, req[0].Object.CS.Bucket, "mztn-sample-bucket")
					gt.Equal(t, req[0].Object.CS.Name, "mydir/GA1ZivRbQAAAyXs.jpg")
					calledLoad++
					return nil
				},
				MockObjectToSources: func(ctx context.Context, input model.Object) ([]*model.Source, error) {
					calledE2S++
					gt.Equal(t, input.CS.Bucket, "mztn-sample-bucket")
					gt.Equal(t, input.CS.Name, "mydir/GA1ZivRbQAAAyXs.jpg")

					return []*model.Source{
						{
							Parser:   types.JSONParser,
							Schema:   "cloudtrail",
							Compress: types.NoCompress,
						},
					}, nil
				},
			}

			srv := server.New(mock)
			r := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			gt.Equal(t, tc.expect, w.Code)
			gt.Equal(t, tc.calledLoad, calledLoad)
			gt.Equal(t, tc.calledE2S, calledE2S)
		})
	}
}
