// Command pdpstub serves the allow-all policy-verifier (PDP) stand-in of
// internal/pdpstub for compose-runtime scenarios.
//
// TEST INFRASTRUCTURE ONLY — never deploy outside a test scenario.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/provin-line/e2e/internal/pdpstub"
)

func main() {
	addr := flag.String("addr", ":9091", "listen address")
	flag.Parse()

	log.Printf("pdpstub (allow-all) listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, pdpstub.Handler()))
}
