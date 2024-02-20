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
	"github.com/m-mizutani/swarm/pkg/usecase"
)

//go:embed testdata/http/pubsub.json
var pubsubBody []byte

func TestEventRequest(t *testing.T) {
	testCases := map[string]struct {
		method string
		path   string
		body   []byte
		expect int
		called int
	}{
		"valid pubsub": {
			method: http.MethodPost,
			path:   "/event/pubsub",
			body:   pubsubBody,
			expect: http.StatusOK,
			called: 1,
		},
		"invalid pubsub": {
			method: http.MethodPost,
			path:   "/event/pubsub",
			body:   []byte("invalid"),
			expect: http.StatusBadRequest,
			called: 0,
		},
		"invalid path": {
			method: http.MethodPost,
			path:   "/invalid",
			body:   []byte("invalid"),
			expect: http.StatusNotFound,
			called: 0,
		},
		"invalid method": {
			method: http.MethodGet,
			path:   "/event/pubsub",
			body:   pubsubBody,
			expect: http.StatusMethodNotAllowed,
			called: 0,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			var called int
			mock := &usecase.Mock{
				MockLoadData: func(ctx context.Context, req *model.LoadDataRequest) error {
					gt.Equal(t, req.CSEvent.Bucket, "mztn-sample-bucket")
					gt.Equal(t, req.CSEvent.ID, "mztn-sample-bucket/mydir/GA1ZivRbQAAAyXs.jpg/1708130907832889")
					called++
					return nil
				},
			}
			srv := server.New(mock)
			r := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			gt.Equal(t, tc.expect, w.Code)
			gt.Equal(t, tc.called, called)
		})
	}
}
