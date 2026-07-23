// Scenario simple: a single-organization source → chained → sink pipeline on
// one real standalone node over a real NATS broker.
//
// Story: an external producer pushes a raw JSON reading; the source loop signs
// a FirstDrop; the chained loop verifies, converts (adds relayed=true), and
// re-signs chain-preserving; the sink verifies and emits NDJSON. The test then
// reads results back over the wire only: sink stdout, ResolveVC, GetAuditStatus,
// the public /did/ resolution route — and re-verifies the sink-consumed
// credential cryptographically with the product's own vc.Verifier.
//
// Runtimes: process (default — subprocess binary + in-harness broker) and
// compose (E2E_RUNTIME=compose — the docker-compose.yml topology with
// provisioning generated into testdata/). Both drive the same assertions.
package simple

import (
	"context"
	"encoding/json"
	"fmt"
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
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"
	"github.com/provin-line/oss/vc"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src"
	srcProcessDID    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src:process:s1"
	relayPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:relay"
	relayProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:relay:process:r1"

	ingressSubject = "ingest.src"
)

// loopsBlock renders the three loops; selfBase is the node's own control-plane
// base URL AS THE NODE REACHES IT (loopback in process mode, its compose
// service name in compose mode).
func loopsBlock(selfBase string) string {
	return fmt.Sprintf(`
      src {
        role            = "source"
        ingress-subject = %q
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "src"
        process-id           = "s1"
        transformation-claim = "convert"
      }
      relay {
        role            = "chained"
        ingress-subject = %q
        chained {
          output-subject = %q
          issuer {
            did                 = %q
            key-id              = "signing"
            verification-method = %q
          }
          pipeline-id           = "relay"
          process-id            = "r1"
          transformation-claim  = "convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
          converter             = "$merge([$, {'relayed': true}])"
        }
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
		ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing",
		srcPipelineDID, relayPipelineDID, relayProcessDID, relayProcessDID+"#signing", selfBase,
		relayPipelineDID, selfBase)
}

func TestSimple_SourceChainedSink(t *testing.T) {
	runScenario(t, harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    []string{srcPipelineDID, relayPipelineDID},
		ProcessDIDs:     []string{srcProcessDID, relayProcessDID},
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject},
	}))
}

// runScenario is the runtime-independent story: stimulate, assert.
// StartSingleNode has already bootstrapped the owner + pipelines + processes
// over the wire (the external-key path on both runtimes —
// see SingleNodeSpec's doc).
func runScenario(t *testing.T, e harness.SingleNodeEnv) {
	ctx := context.Background()

	// Inject one raw JSON reading as an external producer on the account.
	conn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher(ingressSubject).Publish([]byte(`{"reading":42}`)); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	// The sink emits one NDJSON record on the node's stdout.
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
		t.Fatalf("sink confidence = %q, want verified (line payload: %s)", sinkRec.Confidence, sinkRec.Payload)
	}
	var payload map[string]any
	if err := json.Unmarshal(sinkRec.Payload, &payload); err != nil {
		t.Fatalf("sink payload not JSON: %v", err)
	}
	if payload["relayed"] != true {
		t.Errorf("sink payload missing converter mark relayed=true: %v", payload)
	}
	if payload["reading"] != float64(42) {
		t.Errorf("sink payload reading = %v, want 42", payload["reading"])
	}

	// Fetch the sink-consumed credential over the wire and re-verify it with
	// the product's own verifier against the node's public DID resolution route.
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
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(string) (string, error) {
		return e.NodeBase, nil
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	vres, err := verifier.Verify(ctx, &cred)
	if err != nil || vres.Overall != vc.ConfidenceVerified {
		t.Fatalf("independent verify: overall=%v err=%v", vres, err)
	}
	if cred.Issuer() != relayProcessDID {
		t.Errorf("head credential issuer = %q, want %q", cred.Issuer(), relayProcessDID)
	}

	// The async audit runner records a linear-chain verdict for the consumed head.
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
}
