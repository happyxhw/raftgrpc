package raftgrpc

import (
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// HttpKVAPI Handler for a http based key-value store backed by raft
type HttpKVAPI struct {
	store        *KvStore
	confChangeCh chan<- raftpb.ConfChange
	logger       *zap.Logger
}

func NewHttpKVAPI(store *KvStore, confChangeCh chan<- raftpb.ConfChange, logger *zap.Logger) *HttpKVAPI {
	return &HttpKVAPI{
		store:        store,
		confChangeCh: confChangeCh,
		logger:       logger,
	}
}

func (h *HttpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			h.logger.Error("read body", zap.Error(err))
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if v, ok := h.store.LookUp(key); ok {
			_, _ = w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			h.logger.Error("read body", zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			h.logger.Error("convert id", zap.Error(err))
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		h.confChangeCh <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			h.logger.Error("convert id", zap.Error(err))
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeCh <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func ServeHttpKVAPI(kv *KvStore, port int, confChangeCh chan<- raftpb.ConfChange, errorCh <-chan error, logger *zap.Logger) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &HttpKVAPI{
			store:        kv,
			confChangeCh: confChangeCh,
			logger:       logger,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorCh; ok {
		log.Fatal(err)
	}
}
