package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/camdnn/factory-edge-gateway/internal/mtconnect"
	"github.com/camdnn/factory-edge-gateway/internal/sim"
)

// writeXML is a helper function to print out xml formatted data
func writeXML(w http.ResponseWriter, v any) {
	body, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Printf("encode xml: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")

	if _, err = w.Write([]byte(xml.Header)); err != nil {
		log.Printf("write status: %v", err)
	}
	if _, err = w.Write(body); err != nil {
		log.Printf("write status: %v", err)
	}
}

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

	// prints xml formatted data to current path
	http.HandleFunc("GET /current", func(w http.ResponseWriter, r *http.Request) {
		writeXML(w, mtconnect.FromStatus(cnc.Snapshot(time.Now())))
	})

	// start a connection with addr
	log.Printf("simulator (json formatted) listening on http://localhost%s/status (seed %d)", *addr, *seed)
	log.Printf("simulator (xml formatted) listening on http://localhost%s/status (seed %d)", *addr, *seed)
	// Pass nil to use default router
	log.Fatal(http.ListenAndServe(*addr, nil))
}
