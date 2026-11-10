package main

import (
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
)

//go:embed index.html
var indexHTML []byte

const eventsOnBoard = 10

// serve runs the status board: the page itself and the JSON it reads.
func serve(addr string, store *Store, state *pipelineState) {
	internalError := func(w http.ResponseWriter, err error) {
		log.Printf("api: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v)
	}

	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	// everything the board shows, and nothing else
	http.HandleFunc("GET /api/board", func(w http.ResponseWriter, r *http.Request) {
		latest, err := store.Latest()
		if err != nil {
			internalError(w, err)
			return
		}
		lastGap, err := store.LastGap()
		if err != nil {
			internalError(w, err)
			return
		}
		events, err := store.RecentEvents(eventsOnBoard)
		if err != nil {
			internalError(w, err)
			return
		}
		lastChange, err := store.LastStatusChanges()
		if err != nil {
			internalError(w, err)
			return
		}
		machineReceivedAt, err := store.ReceivedAt()
		if err != nil {
			internalError(w, err)
			return
		}
		stored, err := store.Count()
		if err != nil {
			internalError(w, err)
			return
		}

		now := time.Now()
		writeJSON(w, map[string]any{
			"pipeline": pipelineHealth(state.receivedAt(), state.lag.Load(), agentAvailability(latest), lastGap, state.kafkaErr.Load().(string), now),
			"machines": machineStatuses(latest, lastChange, machineReceivedAt, now),
			"events":   events,
			"stored":   stored,
		})
	})

	// one machine's full list of data items, loaded only when its details are opened
	http.HandleFunc("GET /api/machines/{name}", func(w http.ResponseWriter, r *http.Request) {
		latest, err := store.Latest()
		if err != nil {
			internalError(w, err)
			return
		}
		items := []mtconnect.Observation{}
		for _, o := range latest {
			if o.Device == r.PathValue("name") {
				items = append(items, o)
			}
		}
		writeJSON(w, items)
	})

	log.Printf("status board on http://%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
