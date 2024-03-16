package server_test

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-mizutani/gt"
	"github.com/m-mizutani/swarm/pkg/controller/server"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/domain/types"
	"github.com/m-mizutani/swarm/pkg/usecase"
)

//go:embed testdata/http/pubsub.json
var pubsubBody []byte

func TestPubSubCloudStorage(t *testing.T) {
	testCases := map[string]struct {
		method     string
		path       string
		body       []byte
		expect     int
		calledLoad int
		calledE2S  int
	}{
		"valid pubsub cloud storage": {
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

//go:embed testdata/http/pubsub_swarm.json
var pubsubBodySwarm []byte

func TestPubSubSwarmMessage(t *testing.T) {
	var calledLoad, calledE2S int
	mock := &usecase.Mock{
		MockLoadData: func(ctx context.Context, req []*model.LoadRequest) error {
			gt.A(t, req).Must().Length(6)
			gt.Equal(t, req[0].Source.Parser, types.JSONParser)
			gt.Equal(t, req[0].Source.Schema, "cloudtrail")
			gt.Equal(t, req[0].Source.Compress, types.NoCompress)
			gt.Equal(t, req[0].Object.CS.Bucket, "mztn-sample-bucket")
			gt.String(t, string(req[0].Object.CS.Name)).HasSuffix(".json.log.gz")
			calledLoad++
			return nil
		},
		MockObjectToSources: func(ctx context.Context, input model.Object) ([]*model.Source, error) {
			calledE2S++
			gt.Equal(t, input.CS.Bucket, "mztn-sample-bucket")
			gt.String(t, string(input.CS.Name)).HasSuffix(".json.log.gz")

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
	r := httptest.NewRequest("POST", "/event/pubsub/swarm", bytes.NewReader(pubsubBodySwarm))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	gt.Equal(t, http.StatusOK, w.Code)
	gt.Equal(t, 1, calledLoad)
	gt.Equal(t, 6, calledE2S)

}

func TestPubSubMessageBlock(t *testing.T) {
	testCases := map[string]struct {
		calledLoad             int
		calledUpdateState      int
		calledGetOrCreateState int
		acquired               bool
		returnState            types.MsgState
		updateState            types.MsgState
		expectCode             int
		loadError              error
		getOrCreateStateError  error
	}{
		"normal case": {
			calledLoad:             1,
			calledUpdateState:      1,
			calledGetOrCreateState: 1,
			acquired:               true,
			returnState:            types.MsgRunning,
			updateState:            types.MsgCompleted,
			expectCode:             http.StatusOK,
		},
		"already completed": {
			calledLoad:             0,
			calledUpdateState:      0,
			calledGetOrCreateState: 1,
			acquired:               false,
			returnState:            types.MsgCompleted,
			updateState:            types.MsgCompleted,
			expectCode:             http.StatusOK,
		},
		"already acquired": {
			calledLoad:             0,
			calledUpdateState:      0,
			calledGetOrCreateState: 1,
			acquired:               false,
			returnState:            types.MsgRunning,
			updateState:            types.MsgRunning,
			expectCode:             http.StatusResetContent,
		},
		"error on get or create state": {
			calledLoad:             0,
			calledUpdateState:      0,
			calledGetOrCreateState: 1,
			acquired:               false,
			getOrCreateStateError:  errors.New("some error"),
			expectCode:             http.StatusBadRequest,
		},
		"error on load data": {
			calledLoad:             1,
			calledUpdateState:      1,
			calledGetOrCreateState: 1,
			acquired:               true,
			returnState:            types.MsgRunning,
			updateState:            types.MsgFailed,
			loadError:              errors.New("some error"),
			expectCode:             http.StatusBadRequest,
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			var calledGetOrCreateState, calledUpdateState, calledLoad int
			now := time.Now()
			msgID := "10509751019207081"
			mock := &usecase.Mock{
				MockLoadData: func(ctx context.Context, req []*model.LoadRequest) error {
					calledLoad++
					return tc.loadError
				},
				MockGetOrCreateState: func(ctx context.Context, msgType types.MsgType, id string) (*model.State, bool, error) {
					calledGetOrCreateState++
					gt.Equal(t, types.MsgPubSub, msgType)
					gt.Equal(t, id, msgID)
					return &model.State{
						ID:        id,
						State:     tc.returnState,
						RequestID: types.NewRequestID(),
						CreatedAt: now,
						UpdatedAt: now,
						ExpiresAt: now.Add(1 * time.Second),
					}, tc.acquired, tc.getOrCreateStateError
				},
				MockUpdateState: func(ctx context.Context, msgType types.MsgType, id string, state types.MsgState) error {
					calledUpdateState++
					gt.Equal(t, id, msgID)
					gt.Equal(t, tc.updateState, state)
					return nil
				},
			}

			srv := server.New(mock)
			r := httptest.NewRequest("POST", "/event/pubsub/cs", bytes.NewReader(pubsubBody))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)

			gt.Equal(t, w.Code, tc.expectCode)
			gt.Equal(t, calledLoad, tc.calledLoad)
			gt.Equal(t, calledGetOrCreateState, tc.calledGetOrCreateState)
			gt.Equal(t, calledUpdateState, tc.calledUpdateState)
		})
	}
}
