// Package pdpstub is an allow-all policy-verifier (PDP) stand-in for e2e
// scenarios. It answers the o3co policy-verifier wire surface the nodes are
// configured against — POST /verify with {resource, action[, context]} — with
// a whole allow decision envelope, the only answer the protobuf interceptors
// take as an allow, so nodes boot with their production fail-closed auth
// wiring intact while every authorization decision is permitted.
//
// TEST INFRASTRUCTURE ONLY — never deploy outside a test scenario.
package pdpstub

import (
	"encoding/json"
	"net/http"
)

type verifyRequest struct {
	Resource *string `json:"resource"`
	Action   *string `json:"action"`
}

type decision struct {
	Resource string `json:"resource"`
	Action   string `json:"action"`
	Decision string `json:"decision"`
	Reason   reason `json:"reason"`
}

type reason struct {
	Groups []struct{} `json:"groups"`
}

// Handler serves POST /verify, answering an allow for the request's resource
// and action (400 when either is missing or not a string), and GET /healthz.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /verify", func(w http.ResponseWriter, r *http.Request) {
		var req verifyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Resource == nil || req.Action == nil {
			http.Error(w, "want a JSON object with string resource and action", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(decision{
			Resource: *req.Resource,
			Action:   *req.Action,
			Decision: "allow",
			Reason:   reason{Groups: []struct{}{}},
		})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
