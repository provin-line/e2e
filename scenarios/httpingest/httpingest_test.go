// Scenario httpingest: data enters the provenance chain through the apipush
// HTTP surface instead of a raw NATS publish — the deployment shape for data
// sources that speak HTTP but not NATS (finding #2 follow-up).
//
// Story: a single node runs a push-enabled source loop and an observing sink.
// The producer POSTs one JSON reading to /ingest/<loop>/push with an L1
// bearer. The scenario pins the edge contract on the way: the public health
// route is the readiness signal, an unauthenticated POST is 401, a
// duplicate-key body is 400 (strict gate) — and neither rejected push leaves
// any downstream trace (the rejected payloads are distinct, and the sink is
// asserted to have emitted exactly one record). The 202's payload_hash equals
// the issued FirstDrop's input/output hash (the client's correlation handle).
// Downstream is read back over the wire only: sink NDJSON, ResolveVC +
// independent vc.Verifier re-verification, async audit VERIFIED.
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose).
package httpingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/provin-line/oss/crypto/ed25519"
	auditpb "github.com/provin-line/oss/gen/go/dplaax/audit/v1"
	"github.com/provin-line/oss/gen/go/dplaax/audit/v1/auditpbconnect"
	vcpb "github.com/provin-line/oss/gen/go/dplaax/vc/v1"
	"github.com/provin-line/oss/gen/go/dplaax/vc/v1/vcpbconnect"
	"github.com/provin-line/oss/network/pkg/core"
	"github.com/provin-line/oss/network/pkg/didresolver"
	"github.com/provin-line/oss/vc"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:readings"
	srcProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:readings:process:s1"

	// The loop name becomes the /ingest/<name>/ path segment.
	pushLoop   = "src"
	pushPath   = "/ingest/" + pushLoop + "/push"
	healthPath = "/ingest/" + pushLoop + "/health"
	// rawJSON is the one payload that must reach the chain; the rejected
	// probes below use payloads distinct from it, so a rejection that leaked a
	// publish anyway would surface as an extra / mismatched sink record.
	rawJSON        = `{"lot_id":"L-42","weight_kg":120}`
	intruderJSON   = `{"lot_id":"EVE-401","weight_kg":1}`
	ingressSubject = "ingest.readings"
)

// loopsBlock renders a push-enabled source and an observing sink.
func loopsBlock(selfBase string) string {
	return fmt.Sprintf(`
      %s {
        role            = "source"
        ingress-subject = %q
        push-ingress    = true
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "readings"
        process-id           = "s1"
        transformation-claim = "convert"
      }
      archive {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`,
		pushLoop, ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing",
		srcPipelineDID, selfBase)
}

func TestHTTPIngest_PushToVerifiedChain(t *testing.T) {
	runScenario(t, harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:    "acme",
		RegistryID: registryID,
		NodeDID:    ownerDID,
		Loops:      loopsBlock,
		Tunables:   harness.FastTunables,
		// The sink's subscription is gated broker-side; the push-enabled
		// source's readiness is asserted through the product's own health
		// route below (that surface IS the deployment's readiness signal).
		IngressSubjects: []string{srcPipelineDID},
	}))
}

func runScenario(t *testing.T, e harness.SingleNodeEnv) {
	ctx := context.Background()

	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, e.NodeBase, owner, []string{srcPipelineDID}, []string{srcProcessDID})

	// A bounded client so a wedged connection fails the WaitFor tick instead
	// of hanging the whole test past its deadline (harness waitHealthy idiom).
	httpc := &http.Client{Timeout: 5 * time.Second}

	// The push surface's public health route is the readiness signal: it turns
	// 200 only once the loop's broker subscription is confirmed.
	harness.WaitFor(t, "push health 200", 60*time.Second, func() bool {
		resp, err := httpc.Get(e.NodeBase + healthPath)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	post := func(auth, body string) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, e.NodeBase+pushPath, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := httpc.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", pushPath, err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		return resp, string(b)
	}

	// The edge is PDP-guarded: no bearer → 401. (Auth-before-body-gates
	// ordering is pinned product-side; the probe payload is valid JSON and
	// DISTINCT from rawJSON so a rejection that leaked a publish anyway would
	// surface in the exactly-one-sink-record assertion below.)
	if resp, _ := post("", intruderJSON); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated push: got %d, want 401", resp.StatusCode)
	}
	// The strict-JSON gate answers synchronously: a duplicate-key body is 400
	// naming the gate, never a silently errored async event.
	if resp, body := post("Bearer "+harness.BearerToken, `{"a":1,"a":2}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate-key push: got %d, want 400", resp.StatusCode)
	} else if !strings.Contains(body, "strict JSON") {
		t.Fatalf("duplicate-key 400 body = %q, want the strict-JSON gate named", body)
	}

	// The real reading: 202 Accepted with the payload's content address.
	resp, body := post("Bearer "+harness.BearerToken, rawJSON)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("push: got %d (%s), want 202", resp.StatusCode, body)
	}
	var accepted struct {
		PayloadHash string `json:"payload_hash"`
	}
	if err := json.Unmarshal([]byte(body), &accepted); err != nil {
		t.Fatalf("202 body %q: %v", body, err)
	}
	sum := sha256.Sum256([]byte(rawJSON))
	wantHash := "sha256:" + hex.EncodeToString(sum[:])
	if accepted.PayloadHash != wantHash {
		t.Fatalf("payload_hash = %q, want %q", accepted.PayloadHash, wantHash)
	}

	// The sink emits one verified NDJSON record for the pushed payload.
	type sinkRecord struct {
		Credential string          `json:"credential"`
		Confidence string          `json:"confidence"`
		Payload    json.RawMessage `json:"payload"`
	}
	var sinkRec sinkRecord
	harness.WaitFor(t, "sink NDJSON record", 60*time.Second, func() bool {
		for _, line := range e.SinkLines() {
			var rec sinkRecord
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Credential != "" {
				sinkRec = rec
				return true
			}
		}
		return false
	})
	if !strings.EqualFold(sinkRec.Confidence, "verified") {
		t.Fatalf("sink confidence = %q, want verified (payload: %s)", sinkRec.Confidence, sinkRec.Payload)
	}
	var payload map[string]any
	if err := json.Unmarshal(sinkRec.Payload, &payload); err != nil {
		t.Fatalf("sink payload not JSON: %v", err)
	}
	if payload["lot_id"] != "L-42" || payload["weight_kg"] != float64(120) {
		t.Errorf("sink payload = %v, want the pushed reading verbatim", payload)
	}

	// Fetch the consumed credential and close the correlation loop: the
	// FirstDrop's input/output hash IS the 202's payload_hash (verbatim
	// ingestion), and the credential re-verifies independently.
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	resolved, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: sinkRec.Credential})))
	if err != nil {
		t.Fatalf("ResolveVC(%s): %v", sinkRec.Credential, err)
	}
	var cred vc.PipelinePassCredential
	if err := json.Unmarshal(resolved.Msg.GetCredential(), &cred); err != nil {
		t.Fatalf("unmarshal credential: %v", err)
	}
	if got, err := cred.Hash(); err != nil || got != sinkRec.Credential {
		t.Fatalf("resolved credential hash = %q (err %v), want %q", got, err, sinkRec.Credential)
	}
	if cred.Issuer() != srcProcessDID {
		t.Errorf("head credential issuer = %q, want %q", cred.Issuer(), srcProcessDID)
	}
	subj, err := cred.Subject()
	if err != nil {
		t.Fatalf("credential subject: %v", err)
	}
	if subj.InputHash != wantHash || subj.OutputHash != wantHash {
		t.Errorf("credential hashes = %q / %q, want both %q (the 202 correlation handle)", subj.InputHash, subj.OutputHash, wantHash)
	}
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(string) (string, error) {
		return e.NodeBase, nil
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	vres, err := verifier.Verify(ctx, &cred)
	if err != nil || vres.Overall != vc.ConfidenceVerified {
		t.Fatalf("independent verify: overall=%v err=%v", vres, err)
	}

	// The async audit runner records a linear-chain verdict for the head.
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)
	harness.WaitFor(t, "audit VERIFIED for head "+sinkRec.Credential, 60*time.Second, func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{
			HeadHash: sinkRec.Credential,
		})))
		if err != nil {
			return false
		}
		lc := st.Msg.GetLinearChain()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})

	// Rejected pushes leave no trace: the whole story has settled (audit
	// VERIFIED), and the sink emitted exactly ONE credential record — the
	// accepted payload's. A 401/400 that leaked a publish anyway would appear
	// here as an extra record (the probes' payloads are distinct from rawJSON,
	// and the broker delivers in publish order, so a leak cannot hide behind
	// the accepted record).
	var recs []sinkRecord
	for _, line := range e.SinkLines() {
		var rec sinkRecord
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Credential != "" {
			recs = append(recs, rec)
		}
	}
	if len(recs) != 1 {
		t.Fatalf("sink emitted %d credential records, want exactly 1 (rejected pushes must leave no trace): %+v", len(recs), recs)
	}
	if string(recs[0].Payload) != rawJSON {
		t.Errorf("sole sink record payload = %s, want the accepted payload %s", recs[0].Payload, rawJSON)
	}
}
