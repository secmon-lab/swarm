package server

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/m-mizutani/goerr"
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/domain/model"
	"github.com/m-mizutani/swarm/pkg/utils"
)

type Server struct {
	mux *chi.Mux
}

func New(uc interfaces.UseCase) *Server {
	route := chi.NewRouter()

	route.Use(Logging)
	route.Use(Authorization(uc))

	route.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		utils.SafeWrite(w, []byte("OK"))
	})

	route.Route("/event", func(r chi.Router) {
		r.Post("/pubsub", func(w http.ResponseWriter, r *http.Request) {
			if err := handlePubSubEvent(uc, r); err != nil {
				utils.HandleError(r.Context(), "failed handle pubsub event", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusOK)
			utils.SafeWrite(w, []byte("OK"))
		})
	})

	return &Server{
		mux: route,
	}
}

func handlePubSubEvent(uc interfaces.UseCase, r *http.Request) error {
	var msg model.PubSubBody
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return goerr.Wrap(err, "failed to read body")
	}
	if err := json.Unmarshal(body, &msg); err != nil {
		return goerr.Wrap(err, "failed to unmarshal body").With("body", string(body))
	}

	data, err := base64.StdEncoding.DecodeString(msg.Message.Data)
	if err != nil {
		return goerr.Wrap(err, "failed to decode base64").With("data", msg.Message.Data)
	}

	var event model.CloudStorageEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return goerr.Wrap(err, "failed to unmarshal data").With("data", string(data))
	}

	if err := uc.LoadData(r.Context(), &model.LoadDataRequest{
		CSEvent: &event,
	}); err != nil {
		return goerr.Wrap(err).With("event", event)
	}

	return nil
}

/*
func handleDirectStorageEvent(uc interfaces.UseCase, r *http.Request) error {
	var event model.EventarcDirectEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		return goerr.Wrap(err).With("body", r.Body)
	}

	if err := uc.LoadData(r.Context(), &model.LoadDataRequest{
		Bucket:     types.CSBucket(event.Bucket),
		ObjectName: types.CSObjectID(event.Name),
	}); err != nil {
		return goerr.Wrap(err).With("event", event)
	}

	return nil
}
*/

func (x *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	x.mux.ServeHTTP(w, r)
}
