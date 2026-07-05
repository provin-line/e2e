// Scenario simple: a single-organization source → chained → sink pipeline on
// one real standalone node over a real NATS broker.
//
// Story: an external producer pushes a raw JSON reading; the source loop signs
// a FirstDrop; the chained loop verifies, converts (adds relayed=true), and
// re-signs chain-preserving; the sink verifies and emits NDJSON. The test then
// reads results back over the wire only: sink stdout, ResolveVC, GetAuditStatus,
// the public /did/ resolution route — and re-verifies the sink-consumed
// credential cryptographically with the product's own vc.Verifier.
package simple

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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

func loopsBlock(listenAddr string) string {
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
          upstream-endpoint     = "http://127.0.0.1%s"
          converter             = "$merge([$, {'relayed': true}])"
        }
      }
      archive {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = "http://127.0.0.1%s"
        }
      }`,
		ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing",
		srcPipelineDID, relayPipelineDID, relayProcessDID, relayProcessDID+"#signing", listenAddr,
		relayPipelineDID, listenAddr)
}

func TestSimple_SourceChainedSink(t *testing.T) {
	ctx := context.Background()
	bin := harness.BuildStandalone(t)
	listenAddr := harness.FreePort(t)
	pdpURL := harness.StartPDPStub(t, harness.FreePort(t))

	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "acme")
	acme := broker.Account(t, "acme")

	baseURL := "http://127.0.0.1" + listenAddr
	cfg := harness.NodeConfig{
		ListenAddr:      listenAddr,
		RegistryID:      registryID,
		PDPBaseURL:      pdpURL,
		NATSURL:         broker.URL,
		AccountSeedFile: acme.SeedFile,
		TrustSeedFile:   broker.TrustSeedFile,
		ResolverDir:     broker.ResolverDir,
		NodeDID:         ownerDID,
		ResolverBaseURL: baseURL,
		VCStoreEndpoint: baseURL,
		LoopsBlock:      loopsBlock(listenAddr),
		Extra: `    batch-resolver { interval = 1s, batch-size = 64, max-retries = 5, max-depth = 1024 }
    audit-runner { interval = 1s, batch-size = 64, max-attempts = 10 }`,
	}

	nodeDir := filepath.Join(workDir, "acme-node")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	node := harness.StartNode(t, "acme", bin, nodeDir, listenAddr, cfg.Render())

	// Operator bootstrap over the wire: owner + pipelines + processes. The
	// registry mints and holds the process signing keys (KMS model).
	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, node.BaseURL, owner,
		[]string{srcPipelineDID, relayPipelineDID},
		[]string{srcProcessDID, relayProcessDID},
	)

	// Inject one raw JSON reading as an external producer on the account.
	conn, err := natstransport.Connect(natstransport.Config{URL: broker.URL, AccountSeed: acme.Seed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	input := []byte(`{"reading":42}`)
	if err := conn.Publisher(ingressSubject).Publish(input); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	// The sink emits one NDJSON record on the node's stdout.
	var sinkRec struct {
		Credential string          `json:"credential"`
		Confidence string          `json:"confidence"`
		Payload    json.RawMessage `json:"payload"`
	}
	harness.WaitFor(t, "sink NDJSON record", 60*time.Second, func() bool {
		for _, line := range node.SinkLines() {
			var rec struct {
				Credential string          `json:"credential"`
				Confidence string          `json:"confidence"`
				Payload    json.RawMessage `json:"payload"`
			}
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
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, node.BaseURL)
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
		return node.BaseURL, nil
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
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, node.BaseURL)
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
