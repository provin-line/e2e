// Command pdpstub is an allow-all policy-verifier (PDP) stand-in for e2e
// scenarios. It implements the o3co policy-verifier wire surface the provin
// standalone node is configured against (POST {base}/verify → 2xx = allow),
// so nodes boot with their production fail-closed auth wiring intact while
// every authorization decision is permitted.
//
// TEST INFRASTRUCTURE ONLY — never deploy outside a test scenario.
package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	addr := flag.String("addr", ":9091", "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /verify", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("pdpstub (allow-all) listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
