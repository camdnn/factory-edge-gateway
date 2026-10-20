package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/sim"
)

func main() {
	// flags for addr and seed
	addr := flag.String("addr", ":8080", "host address")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "seed value for random number generator")
	flag.Parse() // read cmdl

	// new cnc instance
	cnc := sim.NewCNC("cnc-1", *seed)

	// go routine to step the machine
	// ticker sends ticks for each interval d (.1 second in this case)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			// step .1 seconds at a time
			cnc.Step(100 * time.Millisecond)
		}
	}()

	// Function Go calls for each Request
	// w -> write Response (create json)
	// r -> hold request
	http.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cnc.Snapshot(time.Now())); err != nil {
			log.Printf("write status: %v", err)
		}
	})

	// start a connection with addr
	log.Printf("simulator listening on http://localhost%s/status (seed %d)", *addr, *seed)
	// Pass nil to use default router
	log.Fatal(http.ListenAndServe(*addr, nil))
}
