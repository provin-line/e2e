// Scenario supplychain (real-world use case 1): a THREE-organization
// provenance chain in which every organization runs its own node, hosts its
// own registry, and signs its own hop with keys that never leave its node:
//
//	manufacturer (registry mfg.poc.dplaax.dev):    source loop — signs the lot
//	                                           record as a FirstDrop
//	distributor  (registry dist.poc.dplaax.dev):   chained loop — verifies the
//	                                           manufacturer's credential,
//	                                           stamps its check, re-signs
//	retailer     (registry retail.poc.dplaax.dev): sink loop — verifies, audits
//
// Cross-organization delivery happens only through explicit NATS account
// grants (mfg→dist on the lot subject, dist→retail on the relay subject); an
// ungranted eavesdropper account receives nothing. Cross-registry DID
// resolution uses the registry-base-urls map (oss findings #12 fix) — each
// node maps all three registry ids to node base URLs, and the test-side
// verifier does the same from the host. This lifts the 2-org scope reduction
// the scenario originally shipped with (findings #12/#13).
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
	mfgRegistry    = "mfg.poc.dplaax.dev"
	distRegistry   = "dist.poc.dplaax.dev"
	retailRegistry = "retail.poc.dplaax.dev"

	mfgOwnerDID    = "did:dplaax:mfg.poc.dplaax.dev:org:mfg"
	lotPipelineDID = "did:dplaax:mfg.poc.dplaax.dev:org:mfg:pipeline:lot-emissions"
	lotProcessDID  = "did:dplaax:mfg.poc.dplaax.dev:org:mfg:pipeline:lot-emissions:process:reporter"

	distOwnerDID    = "did:dplaax:dist.poc.dplaax.dev:org:dist"
	distPipelineDID = "did:dplaax:dist.poc.dplaax.dev:org:dist:pipeline:lot-relay"
	distProcessDID  = "did:dplaax:dist.poc.dplaax.dev:org:dist:pipeline:lot-relay:process:checker"

	retailOwnerDID = "did:dplaax:retail.poc.dplaax.dev:org:retail"

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

func distLoops(mfgBase string) string {
	return fmt.Sprintf(`
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
          pipeline-id           = "lot-relay"
          process-id            = "checker"
          transformation-claim  = "convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
          converter             = "$merge([$, {'distributor_checked': true}])"
        }
      }`, lotPipelineDID, distPipelineDID, distProcessDID, distProcessDID+"#signing", mfgBase)
}

func retailLoops(distBase string) string {
	return fmt.Sprintf(`
      intake {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`, distPipelineDID, distBase)
}

// scEnv is what the scenario body needs from either runtime.
type scEnv struct {
	mfgBase    string // manufacturer control plane, host-reachable
	distBase   string // distributor control plane, host-reachable
	retailBase string // retailer control plane, host-reachable
	natsURL    string
	mfgSeed    string
	eveSeed    string
	retailSink func() []string
	// registryURL maps each registry id to a HOST-reachable base URL for the
	// test-side verifier (nodes carry their own container/loopback map).
	registryURL map[string]string
}

func TestSupplyChain_ThreeOrgsOwnRegistries(t *testing.T) {
	ctx := context.Background()
	var e scEnv
	if harness.ComposeRuntime() {
		e = setupCompose(t)
	} else {
		e = setupProcess(t)
	}

	// Operator bootstrap: every producing org registers on ITS OWN node, so
	// its signing keys live only in its own keystore (KMS model per org).
	mfgOwner := harness.NewOwner(t, mfgOwnerDID)
	harness.Bootstrap(t, e.mfgBase, mfgOwner, []string{lotPipelineDID}, []string{lotProcessDID})
	distOwner := harness.NewOwner(t, distOwnerDID)
	harness.Bootstrap(t, e.distBase, distOwner, []string{distPipelineDID}, []string{distProcessDID})

	// The eavesdropper listens on both cross-org subjects in its own account:
	// without grants it must receive nothing. The self-publish control proves
	// the subscriptions can deliver at all.
	eveConn, err := natstransport.Connect(natstransport.Config{URL: e.natsURL, AccountSeed: e.eveSeed})
	if err != nil {
		t.Fatalf("eavesdropper connect: %v", err)
	}
	defer eveConn.Close()
	eveGot := make(chan []byte, 8)
	for _, subj := range []string{lotPipelineDID, distPipelineDID} {
		if err := eveConn.Subscriber(subj).Subscribe(func(b []byte) { eveGot <- b }); err != nil {
			t.Fatalf("eavesdropper subscribe %s: %v", subj, err)
		}
	}
	for _, subj := range []string{lotPipelineDID, distPipelineDID} {
		if err := eveConn.Publisher(subj).Publish([]byte(`{"control":true}`)); err != nil {
			t.Fatalf("eavesdropper control publish %s: %v", subj, err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-eveGot:
		case <-time.After(10 * time.Second):
			t.Fatalf("eavesdropper subscription %d did not deliver its own control message — negative check would be vacuous", i)
		}
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

	// The retailer's sink receives the distributor's re-signed credential.
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
			if p["lot"] != "LOT-2026-07-042" || p["distributor_checked"] != true {
				t.Fatalf("payload missing lot or distributor mark: %v", p)
			}
			head = rec.Credential
			return true
		}
		return false
	})

	// Wire chain walk across ORGANIZATION boundaries: the retailer resolves
	// head (distributor-signed) and its predecessor (manufacturer-signed) from
	// its own VCResolver, verifying each against the ISSUING org's registry
	// via the per-registry map — three registries, three key sets.
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(registry string) (string, error) {
		if base, ok := e.registryURL[registry]; ok {
			return base, nil
		}
		return "", fmt.Errorf("test resolver: unmapped registry %q", registry)
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.retailBase)
	fetch := func(hash string) *vc.PipelinePassCredential {
		// The predecessor lands in the retailer's store via the async batch
		// resolver; poll briefly rather than assuming it beat us here. The
		// last error is surfaced on timeout — a permanent failure (auth,
		// internal) must not masquerade as a slow NotFound.
		var cred vc.PipelinePassCredential
		var lastErr error
		deadline := time.Now().Add(30 * time.Second)
		for {
			resolved, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: hash})))
			if err == nil {
				if err := json.Unmarshal(resolved.Msg.GetCredential(), &cred); err != nil {
					t.Fatalf("unmarshal %s: %v", hash, err)
				}
				return &cred
			}
			lastErr = err
			if time.Now().After(deadline) {
				t.Fatalf("credential %s never became resolvable on retail (30s); last error: %v", hash, lastErr)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	distCred := fetch(head)
	if distCred.Issuer() != distProcessDID {
		t.Errorf("head issuer = %s, want %s", distCred.Issuer(), distProcessDID)
	}
	if r, err := verifier.Verify(ctx, distCred); err != nil || r.Overall != vc.ConfidenceVerified {
		t.Fatalf("distributor credential verify: overall=%v err=%v", r, err)
	}
	mfgHash := distCred.PreviousCredential()
	if mfgHash == "" {
		t.Fatal("distributor credential carries no predecessor")
	}
	mfgCred := fetch(mfgHash)
	if mfgCred.Issuer() != lotProcessDID {
		t.Errorf("origin issuer = %s, want %s", mfgCred.Issuer(), lotProcessDID)
	}
	if r, err := verifier.Verify(ctx, mfgCred); err != nil || r.Overall != vc.ConfidenceVerified {
		t.Fatalf("manufacturer credential verify: overall=%v err=%v", r, err)
	}
	if mfgCred.PreviousCredential() != "" {
		t.Errorf("origin credential unexpectedly carries a predecessor")
	}

	// The retailer's async audit verifies the full cross-org chain.
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
	// observed nothing on either cross-org subject.
	select {
	case b := <-eveGot:
		t.Fatalf("eavesdropper received %d bytes without a grant", len(b))
	default:
	}
}

// setupProcess boots the three nodes as subprocesses over the in-harness broker.
func setupProcess(t *testing.T) scEnv {
	bin := harness.BuildStandalone(t)

	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"),
		"manufacturer", "distributor", "retailer", "eavesdropper")
	mfgAcc := broker.Account(t, "manufacturer")
	distAcc := broker.Account(t, "distributor")
	retailAcc := broker.Account(t, "retailer")
	eveAcc := broker.Account(t, "eavesdropper")

	// The supply chain IS the grant chain: each producer exports its pipeline
	// subject to exactly the next org.
	broker.Grant(t, "manufacturer", "distributor", lotPipelineDID)
	broker.Grant(t, "distributor", "retailer", distPipelineDID)

	mfgListen, distListen, retailListen := harness.FreePort(t), harness.FreePort(t), harness.FreePort(t)
	mfgBase := "http://127.0.0.1" + mfgListen
	distBase := "http://127.0.0.1" + distListen
	retailBase := "http://127.0.0.1" + retailListen
	regURLs := map[string]string{
		mfgRegistry:    mfgBase,
		distRegistry:   distBase,
		retailRegistry: retailBase,
	}
	pdp := harness.StartPDPStub(t, harness.FreePort(t))

	startNode := func(name, listen, registryID, nodeDID, vcStore, loops, extra string, acc *harness.NATSAccount) *harness.Node {
		dir := filepath.Join(workDir, name+"-node")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		return harness.StartNode(t, name, bin, dir, listen, harness.NodeConfig{
			AllowLoopback:    true,
			ListenAddr:       listen,
			RegistryID:       registryID,
			PDPBaseURL:       pdp,
			NATSURL:          broker.URL,
			AccountSeedFile:  acc.SeedFile,
			TrustSeedFile:    broker.TrustSeedFile,
			ResolverDir:      broker.ResolverDir,
			NodeDID:          nodeDID,
			RegistryBaseURLs: regURLs,
			VCStoreEndpoint:  vcStore,
			LoopsBlock:       loops,
			Extra:            extra,
		}.Render())
	}

	startNode("manufacturer", mfgListen, mfgRegistry, mfgOwnerDID, mfgBase, mfgLoops(), "", mfgAcc)
	startNode("distributor", distListen, distRegistry, distOwnerDID, distBase, distLoops(mfgBase), harness.FastTunables, distAcc)
	retailNode := startNode("retailer", retailListen, retailRegistry, retailOwnerDID, "", retailLoops(distBase), harness.FastTunables, retailAcc)

	// Gate every hop's subscription, not just the first: each hop is a
	// fire-and-forget core-NATS publish, and /healthz does not imply the
	// data-plane loops have subscribed. Gated in setup, BEFORE the scenario
	// body connects the eavesdropper (whose subscriptions on the same
	// subjects must not satisfy these gates).
	broker.WaitForSubscriber(t, ingressSubject, 30*time.Second)
	broker.WaitForSubscriber(t, lotPipelineDID, 30*time.Second)
	broker.WaitForSubscriber(t, distPipelineDID, 30*time.Second)

	return scEnv{
		mfgBase:     mfgBase,
		distBase:    distBase,
		retailBase:  retailBase,
		natsURL:     broker.URL,
		mfgSeed:     mfgAcc.Seed,
		eveSeed:     eveAcc.Seed,
		retailSink:  retailNode.SinkLines,
		registryURL: regURLs,
	}
}

// setupCompose provisions testdata/ (grants BEFORE the broker config renders)
// and boots the three-node docker-compose topology.
func setupCompose(t *testing.T) scEnv {
	scenarioDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testdata := filepath.Join(scenarioDir, "testdata")
	if err := os.RemoveAll(testdata); err != nil {
		t.Fatal(err)
	}

	prov := harness.ProvisionCompose(t, testdata,
		"manufacturer", "distributor", "retailer", "eavesdropper")
	prov.Grant(t, "manufacturer", "distributor", lotPipelineDID)
	prov.Grant(t, "distributor", "retailer", distPipelineDID)
	prov.WriteBrokerConfig(t)

	regURLsInNet := map[string]string{
		mfgRegistry:    "http://mfg:8443",
		distRegistry:   "http://dist:8443",
		retailRegistry: "http://retail:8443",
	}
	writeNode := func(node, account, registryID, nodeDID, vcStore, loops, extra string) {
		prov.WriteNodeConfig(t, node, harness.NodeConfig{
			AllowPrivateNetworks: true,
			ListenAddr:           ":8443",
			RegistryID:           registryID,
			PDPBaseURL:           "http://pdpstub:9091",
			NATSURL:              "nats://nats:4222",
			AccountSeedFile:      "/app/secrets/" + account + "-account.seed",
			TrustSeedFile:        "/app/secrets/operator.seed",
			ResolverDir:          "/app/jwts",
			NodeDID:              nodeDID,
			RegistryBaseURLs:     regURLsInNet,
			VCStoreEndpoint:      vcStore,
			LoopsBlock:           loops,
			Extra:                extra,
		})
	}
	writeNode("mfg", "manufacturer", mfgRegistry, mfgOwnerDID, "http://mfg:8443", mfgLoops(), "")
	writeNode("dist", "distributor", distRegistry, distOwnerDID, "http://dist:8443", distLoops("http://mfg:8443"), harness.FastTunables)
	writeNode("retail", "retailer", retailRegistry, retailOwnerDID, "", retailLoops("http://dist:8443"), harness.FastTunables)

	c := harness.ComposeUp(t, scenarioDir)
	natsMon := c.Port(t, "nats", 8222)
	harness.WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)
	mfgBase := "http://" + c.Port(t, "mfg", 8443)
	harness.WaitHTTPHealthy(t, "mfg", mfgBase+"/healthz", 60*time.Second)
	distBase := "http://" + c.Port(t, "dist", 8443)
	harness.WaitHTTPHealthy(t, "dist", distBase+"/healthz", 60*time.Second)
	retailBase := "http://" + c.Port(t, "retail", 8443)
	harness.WaitHTTPHealthy(t, "retail", retailBase+"/healthz", 60*time.Second)
	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, ingressSubject, 60*time.Second)
	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, lotPipelineDID, 60*time.Second)
	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, distPipelineDID, 60*time.Second)

	readSeed := func(name string) string {
		b, err := os.ReadFile(filepath.Join(testdata, name+"-account.seed"))
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(b))
	}
	return scEnv{
		mfgBase:    mfgBase,
		distBase:   distBase,
		retailBase: retailBase,
		natsURL:    "nats://" + c.Port(t, "nats", 4222),
		mfgSeed:    readSeed("manufacturer"),
		eveSeed:    readSeed("eavesdropper"),
		retailSink: func() []string { return c.SinkLines(t, "retail") },
		registryURL: map[string]string{
			mfgRegistry:    mfgBase,
			distRegistry:   distBase,
			retailRegistry: retailBase,
		},
	}
}
