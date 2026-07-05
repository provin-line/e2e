// Scenario supplychain (real-world use case 1): cross-organization provenance
// handoff. A manufacturer publishes signed emission readings for a production
// lot; a retailer — a different organization on a different node with a
// different NATS account — consumes them only through an explicit cross-account
// grant, verifies the manufacturer's credential, and audits the chain
// asynchronously. A third organization without a grant receives nothing.
//
// Topology (two real nodes, one broker):
//
//	manufacturer node: registry host + source loop (its DIDs and signing keys
//	                   live in its own registry/keystore)
//	retailer node:     sink loop only; resolves ALL DIDs against the
//	                   manufacturer's registry (single-registry override) and
//	                   fetches predecessors from the manufacturer's VCResolver
//	eavesdropper:      a provisioned account with NO import grant
//
// Deliberate scope (finding #12/#13 in the e2e findings log): each org signing
// on its own node requires per-registry resolution mapping, which provin does
// not expose in config yet — so the producing org hosts the registry and the
// consuming org verifies. That is the honest deployable shape today.
package supplychain

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

	mfgOwnerDID    = "did:dplaax:poc.dplaax.dev:org:mfg"
	lotPipelineDID = "did:dplaax:poc.dplaax.dev:org:mfg:pipeline:lot-emissions"
	lotProcessDID  = "did:dplaax:poc.dplaax.dev:org:mfg:pipeline:lot-emissions:process:reporter"

	retailerOwnerDID = "did:dplaax:poc.dplaax.dev:org:retailer"

	ingressSubject = "ingest.lot-emissions"
)

func mfgLoops() string {
	return fmt.Sprintf(`
      reporter {
        role            = "source"
        ingress-subject = %q
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "lot-emissions"
        process-id           = "reporter"
        transformation-claim = "generate"
      }`, ingressSubject, lotPipelineDID, lotProcessDID, lotProcessDID+"#signing")
}

func retailerLoops(mfgBaseURL string) string {
	return fmt.Sprintf(`
      intake {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`, lotPipelineDID, mfgBaseURL)
}

// scEnv is what the scenario body needs from either runtime.
type scEnv struct {
	mfgBase    string // manufacturer control plane, host-reachable
	retailBase string // retailer control plane, host-reachable
	natsURL    string
	mfgSeed    string
	eveSeed    string
	retailSink func() []string
}

func TestSupplyChain_CrossOrgGrantAndAudit(t *testing.T) {
	ctx := context.Background()
	var e scEnv
	if harness.ComposeRuntime() {
		e = setupCompose(t)
	} else {
		e = setupProcess(t)
	}

	// Operator bootstrap on the manufacturer's registry.
	mfgOwner := harness.NewOwner(t, mfgOwnerDID)
	harness.Bootstrap(t, e.mfgBase, mfgOwner, []string{lotPipelineDID}, []string{lotProcessDID})

	// The eavesdropper listens on the same subject in its own account: without
	// an import grant it must receive nothing (org isolation is the product
	// property this use case sells).
	eveConn, err := natstransport.Connect(natstransport.Config{URL: e.natsURL, AccountSeed: e.eveSeed})
	if err != nil {
		t.Fatalf("eavesdropper connect: %v", err)
	}
	defer eveConn.Close()
	eveGot := make(chan []byte, 8)
	if err := eveConn.Subscriber(lotPipelineDID).Subscribe(func(b []byte) { eveGot <- b }); err != nil {
		t.Fatalf("eavesdropper subscribe: %v", err)
	}
	// Positive control: eve's subscription must be able to deliver at all
	// (a self-publish within her own account), so the later empty-channel
	// negative proves isolation, not a dead subscription.
	if err := eveConn.Publisher(lotPipelineDID).Publish([]byte(`{"control":true}`)); err != nil {
		t.Fatalf("eavesdropper control publish: %v", err)
	}
	select {
	case <-eveGot:
	case <-time.After(10 * time.Second):
		t.Fatal("eavesdropper subscription did not deliver its own control message — negative check would be vacuous")
	}

	// The manufacturer's plant system reports one lot's emission record.
	mfgConn, err := natstransport.Connect(natstransport.Config{URL: e.natsURL, AccountSeed: e.mfgSeed})
	if err != nil {
		t.Fatalf("manufacturer connect: %v", err)
	}
	defer mfgConn.Close()
	lot := []byte(`{"lot":"LOT-2026-07-042","co2e_kg":12.5,"site":"osaka-plant-1"}`)
	if err := mfgConn.Publisher(ingressSubject).Publish(lot); err != nil {
		t.Fatalf("publish lot record: %v", err)
	}

	// The retailer's sink receives, verifies, and emits the record.
	var head string
	harness.WaitFor(t, "retailer sink record", 60*time.Second, func() bool {
		for _, line := range e.retailSink() {
			var rec struct {
				Credential string          `json:"credential"`
				Confidence string          `json:"confidence"`
				Payload    json.RawMessage `json:"payload"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
				continue
			}
			if !strings.EqualFold(rec.Confidence, "verified") {
				t.Fatalf("retailer sink record not verified: %s", line)
			}
			var p map[string]any
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				t.Fatalf("payload: %v", err)
			}
			if p["lot"] != "LOT-2026-07-042" {
				t.Fatalf("payload lot = %v", p["lot"])
			}
			head = rec.Credential
			return true
		}
		return false
	})

	// The retailer re-verifies the manufacturer's credential independently,
	// resolving the issuer from the manufacturer's public /did/ route.
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(string) (string, error) {
		return e.mfgBase, nil
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	credBytes := fetchCredential(t, ctx, e.retailBase, head)
	var cred vc.PipelinePassCredential
	if err := json.Unmarshal(credBytes, &cred); err != nil {
		t.Fatalf("unmarshal credential: %v", err)
	}
	if r, err := verifier.Verify(ctx, &cred); err != nil || r.Overall != vc.ConfidenceVerified {
		t.Fatalf("retailer-side verify: overall=%v err=%v", r, err)
	}
	if cred.Issuer() != lotProcessDID {
		t.Errorf("issuer = %s, want %s", cred.Issuer(), lotProcessDID)
	}

	// The retailer's async audit records VERIFIED for the consumed head.
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.retailBase)
	harness.WaitFor(t, "retailer audit VERIFIED", 60*time.Second, func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: head})))
		if err != nil {
			return false
		}
		lc := st.Msg.GetLinearChain()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})

	// Org isolation: beyond her own control message, the ungranted account
	// observed nothing throughout the scenario's forced round-trips.
	select {
	case b := <-eveGot:
		t.Fatalf("eavesdropper received %d bytes on %s without a grant", len(b), lotPipelineDID)
	default:
	}
}

// setupProcess boots both nodes as subprocesses over the in-harness broker.
func setupProcess(t *testing.T) scEnv {
	bin := harness.BuildStandalone(t)

	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "manufacturer", "retailer", "eavesdropper")
	mfgAcc := broker.Account(t, "manufacturer")
	retailAcc := broker.Account(t, "retailer")
	eveAcc := broker.Account(t, "eavesdropper")

	// The supply-chain handoff is the grant: manufacturer exports the lot
	// pipeline subject; the retailer imports it. The eavesdropper gets nothing.
	broker.Grant(t, "manufacturer", "retailer", lotPipelineDID)

	mfgListen := harness.FreePort(t)
	mfgBase := "http://127.0.0.1" + mfgListen
	mfgPDP := harness.StartPDPStub(t, harness.FreePort(t))
	mfgDir := filepath.Join(workDir, "mfg-node")
	if err := os.MkdirAll(mfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mfgNode := harness.StartNode(t, "manufacturer", bin, mfgDir, mfgListen, harness.NodeConfig{
		AllowLoopback:   true,
		ListenAddr:      mfgListen,
		RegistryID:      registryID,
		PDPBaseURL:      mfgPDP,
		NATSURL:         broker.URL,
		AccountSeedFile: mfgAcc.SeedFile,
		TrustSeedFile:   broker.TrustSeedFile,
		ResolverDir:     broker.ResolverDir,
		NodeDID:         mfgOwnerDID,
		ResolverBaseURL: mfgBase,
		VCStoreEndpoint: mfgBase,
		LoopsBlock:      mfgLoops(),
	}.Render())

	retailListen := harness.FreePort(t)
	retailPDP := harness.StartPDPStub(t, harness.FreePort(t))
	retailDir := filepath.Join(workDir, "retail-node")
	if err := os.MkdirAll(retailDir, 0o755); err != nil {
		t.Fatal(err)
	}
	retailNode := harness.StartNode(t, "retailer", bin, retailDir, retailListen, harness.NodeConfig{
		AllowLoopback:   true,
		ListenAddr:      retailListen,
		RegistryID:      registryID,
		PDPBaseURL:      retailPDP,
		NATSURL:         broker.URL,
		AccountSeedFile: retailAcc.SeedFile,
		TrustSeedFile:   broker.TrustSeedFile,
		ResolverDir:     broker.ResolverDir,
		NodeDID:         retailerOwnerDID,
		ResolverBaseURL: mfgBase, // cross-org verification resolves against the producer's registry
		LoopsBlock:      retailerLoops(mfgBase),
		Extra:           harness.FastTunables,
	}.Render())

	broker.WaitForSubscriber(t, ingressSubject, 30*time.Second)
	return scEnv{
		mfgBase:    mfgNode.BaseURL,
		retailBase: retailNode.BaseURL,
		natsURL:    broker.URL,
		mfgSeed:    mfgAcc.Seed,
		eveSeed:    eveAcc.Seed,
		retailSink: retailNode.SinkLines,
	}
}

// setupCompose provisions testdata/ (grants BEFORE the broker config is
// rendered — the compose broker preloads static claims) and boots the two-node
// docker-compose topology.
func setupCompose(t *testing.T) scEnv {
	scenarioDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testdata := filepath.Join(scenarioDir, "testdata")
	if err := os.RemoveAll(testdata); err != nil {
		t.Fatal(err)
	}

	prov := harness.ProvisionCompose(t, testdata, "manufacturer", "retailer", "eavesdropper")
	prov.Grant(t, "manufacturer", "retailer", lotPipelineDID)
	prov.WriteBrokerConfig(t)

	const mfgSelf = "http://mfg:8443"
	prov.WriteNodeConfig(t, "mfg", harness.NodeConfig{
		AllowPrivateNetworks: true,
		ListenAddr:           ":8443",
		RegistryID:           registryID,
		PDPBaseURL:           "http://pdpstub:9091",
		NATSURL:              "nats://nats:4222",
		AccountSeedFile:      "/app/secrets/manufacturer-account.seed",
		TrustSeedFile:        "/app/secrets/operator.seed",
		ResolverDir:          "/app/jwts",
		NodeDID:              mfgOwnerDID,
		ResolverBaseURL:      mfgSelf,
		VCStoreEndpoint:      mfgSelf,
		LoopsBlock:           mfgLoops(),
	})
	prov.WriteNodeConfig(t, "retail", harness.NodeConfig{
		AllowPrivateNetworks: true,
		ListenAddr:           ":8443",
		RegistryID:           registryID,
		PDPBaseURL:           "http://pdpstub:9091",
		NATSURL:              "nats://nats:4222",
		AccountSeedFile:      "/app/secrets/retailer-account.seed",
		TrustSeedFile:        "/app/secrets/operator.seed",
		ResolverDir:          "/app/jwts",
		NodeDID:              retailerOwnerDID,
		ResolverBaseURL:      mfgSelf, // cross-org verification resolves against the producer's registry
		LoopsBlock:           retailerLoops(mfgSelf),
		Extra:                harness.FastTunables,
	})

	c := harness.ComposeUp(t, scenarioDir)
	natsMon := c.Port(t, "nats", 8222)
	harness.WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)
	mfgBase := "http://" + c.Port(t, "mfg", 8443)
	harness.WaitHTTPHealthy(t, "mfg", mfgBase+"/healthz", 60*time.Second)
	retailBase := "http://" + c.Port(t, "retail", 8443)
	harness.WaitHTTPHealthy(t, "retail", retailBase+"/healthz", 60*time.Second)

	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, ingressSubject, 60*time.Second)

	readSeed := func(name string) string {
		b, err := os.ReadFile(filepath.Join(testdata, name+"-account.seed"))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(b))
	}
	return scEnv{
		mfgBase:    mfgBase,
		retailBase: retailBase,
		natsURL:    "nats://" + c.Port(t, "nats", 4222),
		mfgSeed:    readSeed("manufacturer"),
		eveSeed:    readSeed("eavesdropper"),
		retailSink: func() []string { return c.SinkLines(t, "retail") },
	}
}

// fetchCredential resolves a credential by content address from a node's
// VCResolverService.
func fetchCredential(t *testing.T, ctx context.Context, baseURL, hash string) []byte {
	t.Helper()
	client := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, baseURL)
	resolved, err := client.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: hash})))
	if err != nil {
		t.Fatalf("ResolveVC(%s) on %s: %v", hash, baseURL, err)
	}
	return resolved.Msg.GetCredential()
}
