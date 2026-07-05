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
	"os"
	"path/filepath"
	"strconv"
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

const tunables = `    batch-resolver { interval = 1s, batch-size = 64, max-retries = 5, max-depth = 1024 }
    audit-runner { interval = 1s, batch-size = 64, max-attempts = 10 }`

// env is what the shared scenario body needs from either runtime.
type env struct {
	nodeBase  string // control-plane base URL, host-reachable
	natsURL   string // broker URL, host-reachable
	acctSeed  string // acme account seed for the producer connection
	sinkLines func() []string
}

func TestSimple_SourceChainedSink(t *testing.T) {
	if harness.ComposeRuntime() {
		runScenario(t, setupCompose(t))
		return
	}
	runScenario(t, setupProcess(t))
}

// setupProcess boots the process runtime: in-harness broker + subprocess node.
func setupProcess(t *testing.T) env {
	bin := harness.BuildStandalone(t)
	listenAddr := harness.FreePort(t)
	pdpURL := harness.StartPDPStub(t, harness.FreePort(t))

	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "acme")
	acme := broker.Account(t, "acme")

	baseURL := "http://127.0.0.1" + listenAddr
	cfg := harness.NodeConfig{
		AllowLoopback:   true,
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
		LoopsBlock:      loopsBlock(baseURL),
		Extra:           tunables,
	}

	nodeDir := filepath.Join(workDir, "acme-node")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	node := harness.StartNode(t, "acme", bin, nodeDir, listenAddr, cfg.Render())
	return env{
		nodeBase:  node.BaseURL,
		natsURL:   broker.URL,
		acctSeed:  acme.Seed,
		sinkLines: node.SinkLines,
	}
}

// setupCompose provisions testdata/, boots the docker-compose topology, and
// adapts it to env. Requires the images from `make docker-build`.
func setupCompose(t *testing.T) env {
	scenarioDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testdata := filepath.Join(scenarioDir, "testdata")
	if err := os.RemoveAll(testdata); err != nil {
		t.Fatal(err)
	}

	prov := harness.ProvisionCompose(t, testdata, "acme")
	prov.WriteBrokerConfig(t)

	const selfBase = "http://acme:8443"
	prov.WriteNodeConfig(t, "acme", harness.NodeConfig{
		AllowPrivateNetworks: true,
		ListenAddr:           ":8443",
		RegistryID:           registryID,
		PDPBaseURL:           "http://pdpstub:9091",
		NATSURL:              "nats://nats:4222",
		AccountSeedFile:      "/app/secrets/acme-account.seed",
		TrustSeedFile:        "/app/secrets/operator.seed",
		ResolverDir:          "/app/jwts",
		NodeDID:              ownerDID,
		ResolverBaseURL:      selfBase,
		VCStoreEndpoint:      selfBase,
		LoopsBlock:           loopsBlock(selfBase),
		Extra:                tunables,
	})

	c := harness.ComposeUp(t, scenarioDir, "e2e-simple-"+strconv.Itoa(os.Getpid()))
	natsMon := c.Port(t, "nats", 8222)
	harness.WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)
	acmeAddr := c.Port(t, "acme", 8443)
	nodeBase := "http://" + acmeAddr
	harness.WaitHTTPHealthy(t, "acme", nodeBase+"/healthz", 60*time.Second)

	seed, err := os.ReadFile(filepath.Join(testdata, "acme-account.seed"))
	if err != nil {
		t.Fatal(err)
	}
	return env{
		nodeBase:  nodeBase,
		natsURL:   "nats://" + c.Port(t, "nats", 4222),
		acctSeed:  strings.TrimSpace(string(seed)),
		sinkLines: func() []string { return c.SinkLines(t, "acme") },
	}
}

// runScenario is the runtime-independent story: bootstrap, stimulate, assert.
func runScenario(t *testing.T, e env) {
	ctx := context.Background()

	// Operator bootstrap over the wire: owner + pipelines + processes. The
	// registry mints and holds the process signing keys (KMS model).
	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, e.nodeBase, owner,
		[]string{srcPipelineDID, relayPipelineDID},
		[]string{srcProcessDID, relayProcessDID},
	)

	// Inject one raw JSON reading as an external producer on the account.
	conn, err := natstransport.Connect(natstransport.Config{URL: e.natsURL, AccountSeed: e.acctSeed})
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
		for _, line := range e.sinkLines() {
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
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.nodeBase)
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
		return e.nodeBase, nil
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
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.nodeBase)
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
