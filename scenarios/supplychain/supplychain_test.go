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
	// retailer runs no producing/chained loop of its own (only a sink), so —
	// same reasoning as losswindow's retail org — it gets a dedicated
	// pipeline+process pair purely as its node identity: cmd/pipeline's
	// wireauth-signed RegisterAuditHead fires for every consumed head
	// (including a sink's) and is a boot preflight. The all-in-one topology
	// never needed this (no wire boundary between control and data plane), so
	// retailOwnerDID itself was never even registered before this migration.
	retailNodePipelineDID = "did:dplaax:retail.poc.dplaax.dev:org:retail:pipeline:node"
	retailNodeProcessDID  = retailNodePipelineDID + ":process:n1"

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

	// Operator bootstrap (mfg/dist/retail owners + pipelines/processes) has
	// already happened inside setupProcess/setupCompose — the external-key
	// path (both runtimes) needs each org's PIPELINE data dir, which only
	// the runtime-specific setup has (see either setup func's own doc).

	// The eavesdropper listens on both cross-org subjects in its own account:
	// without grants it must receive nothing. The self-publish control proves
	// the subscriptions can deliver at all.
	eveConn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.natsURL, AccountSeed: e.eveSeed})
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
	mfgConn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.natsURL, AccountSeed: e.mfgSeed})
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

// setupProcess boots the three orgs as separated (cmd/network + cmd/pipeline)
// subprocess pairs over the in-harness broker — the separated topology's
// twin of the retiring all-in-one cmd/standalone boot this used to do
// (A2; the compose runtime's own migration is a recorded follow-up,
// AGENTS.md rule 3 — setupCompose below is untouched).
func setupProcess(t *testing.T) scEnv {
	networkBin, pipelineBin := harness.BuildBinaries(t)

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

	mfgNetworkListen, mfgPipelineListen := harness.FreePort(t), harness.FreePort(t)
	distNetworkListen, distPipelineListen := harness.FreePort(t), harness.FreePort(t)
	retailNetworkListen, retailPipelineListen := harness.FreePort(t), harness.FreePort(t)
	mfgBase := "http://127.0.0.1" + mfgNetworkListen
	distBase := "http://127.0.0.1" + distNetworkListen
	retailBase := "http://127.0.0.1" + retailNetworkListen
	regURLs := map[string]string{
		mfgRegistry:    mfgBase,
		distRegistry:   distBase,
		retailRegistry: retailBase,
	}
	pdp := harness.StartPDPStub(t, harness.FreePort(t))

	// startOrg boots one org's separated pair and bootstraps ownerDID + every
	// DID in pipelines/processes over the external-key path
	// (ProvisionExternalIdentity into the PIPELINE's own data dir, then
	// BootstrapExternal) — mirrors losswindow's newOrgNode, minus the
	// restart-safe boot/bootstrap split (no org restarts in this scenario).
	// pipelines MUST be provisioned before processes: see losswindow's
	// newOrgNode doc for why (filestore's DID->directory nesting).
	startOrg := func(name, networkListen, pipelineListen, registryID, ownerDID, nodeDID, tunables, loops string, pipelines, processes []string, acc *harness.NATSAccount) *harness.SeparatedNode {
		networkDir := filepath.Join(workDir, name+"-network")
		pipelineDir := filepath.Join(workDir, name+"-pipeline")
		pipelineDataDir := filepath.Join(pipelineDir, "data")

		extKeys := make(map[string]harness.ExternalKeys, len(pipelines)+len(processes))
		for _, d := range pipelines {
			extKeys[d] = harness.ProvisionExternalIdentity(t, pipelineDataDir, d)
		}
		for _, d := range processes {
			extKeys[d] = harness.ProvisionExternalIdentity(t, pipelineDataDir, d)
		}

		networkCfg, pipelineCfg := harness.SplitNodeConfig(harness.SeparatedConfig{
			NetworkListenAddr:  networkListen,
			PipelineListenAddr: pipelineListen,
			RegistryID:         registryID,
			PDPBaseURL:         pdp,
			NATSURL:            broker.URL,
			AccountSeedFile:    acc.SeedFile,
			TrustSeedFile:      broker.TrustSeedFile,
			ResolverDir:        broker.ResolverDir,
			NodeDID:            nodeDID,
			RegistryBaseURLs:   regURLs,
			AllowLoopback:      true,
			LoopsBlock:         loops,
			Tunables:           tunables,
		})
		sn := harness.StartSeparatedNode(t, harness.SeparatedNodeSpec{
			Name:               name,
			NetworkBin:         networkBin,
			NetworkListenAddr:  networkListen,
			NetworkDir:         networkDir,
			NetworkConfig:      networkCfg,
			PipelineBin:        pipelineBin,
			PipelineListenAddr: pipelineListen,
			PipelineDir:        pipelineDir,
			PipelineConfig:     pipelineCfg,
		})

		owner := harness.NewOwner(t, ownerDID)
		harness.BootstrapExternal(t, sn.BaseURL, owner, pipelines, processes, extKeys)
		return sn
	}

	startOrg("manufacturer", mfgNetworkListen, mfgPipelineListen, mfgRegistry, mfgOwnerDID, lotProcessDID, "", mfgLoops(),
		[]string{lotPipelineDID}, []string{lotProcessDID}, mfgAcc)
	startOrg("distributor", distNetworkListen, distPipelineListen, distRegistry, distOwnerDID, distProcessDID, harness.FastTunables, distLoops(mfgBase),
		[]string{distPipelineDID}, []string{distProcessDID}, distAcc)
	retailNode := startOrg("retailer", retailNetworkListen, retailPipelineListen, retailRegistry, retailOwnerDID, retailNodeProcessDID, harness.FastTunables, retailLoops(distBase),
		[]string{retailNodePipelineDID}, []string{retailNodeProcessDID}, retailAcc)

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
// and boots the three-org docker-compose topology — SIX node services
// (mfg/dist/retail, each split into its own -network + -pipeline pair) plus
// nats + pdpstub — the separated topology's compose twin of setupProcess,
// mirrored org-for-org (A3; AGENTS.md rule 3: both runtimes describe the
// same node/config layout). Bootstrap moves onto the external-key path here
// too (same reason setupProcess's own doc gives): a separated pipeline needs
// its wireauth-signing identity's private half in ITS OWN data dir, which a
// registry-side mint (Bootstrap/mint mode) can never reach (D9 keystore
// locality). retailer — no producing/chained loop of its own — now gets a
// registered owner AND a dedicated node pipeline/process pair for the SAME
// reason losswindow's retail org and setupProcess's retailNode do: its
// pipeline's wireauth-signed RegisterAuditHead (fired for every consumed
// head, including a sink's) is a boot preflight once there is a wire
// boundary to cross — which the compose runtime now has too, unlike before
// this migration.
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
		mfgRegistry:    "http://mfg-network:8443",
		distRegistry:   "http://dist-network:8443",
		retailRegistry: "http://retail-network:8443",
	}

	// writeOrg provisions one org's pipeline-local keys (external-key path,
	// D9 keystore locality — same as setupProcess's startOrg), splits its
	// config via SplitNodeConfig, and writes both halves under testdata — the
	// compose-runtime twin of startOrg, minus actually starting the
	// processes (ComposeUp starts every org's containers together, below).
	writeOrg := func(node, account, registryID, nodeDID, loops, extra string, pipelines, processes []string) map[string]harness.ExternalKeys {
		networkNode := node + "-network"
		pipelineNode := node + "-pipeline"
		pipelineDataDir := filepath.Join(testdata, pipelineNode, "data")

		extKeys := make(map[string]harness.ExternalKeys, len(pipelines)+len(processes))
		for _, d := range pipelines {
			extKeys[d] = harness.ProvisionExternalIdentity(t, pipelineDataDir, d)
		}
		for _, d := range processes {
			extKeys[d] = harness.ProvisionExternalIdentity(t, pipelineDataDir, d)
		}
		harness.MakeContainerReadable(t, filepath.Join(pipelineDataDir, "keys"))

		networkCfg, pipelineCfg := harness.SplitNodeConfig(harness.SeparatedConfig{
			NetworkListenAddr:    ":8443",
			PipelineListenAddr:   ":8443",
			RegistryID:           registryID,
			PDPBaseURL:           "http://pdpstub:9091",
			NATSURL:              "nats://nats:4222",
			AccountSeedFile:      "/app/secrets/" + account + "-account.seed",
			TrustSeedFile:        "/app/secrets/operator.seed",
			ResolverDir:          "/app/jwts",
			NodeDID:              nodeDID,
			RegistryBaseURLs:     regURLsInNet,
			AllowPrivateNetworks: true,
			LoopsBlock:           loops,
			Tunables:             extra,
		})
		prov.WriteNodeConfig(t, networkNode, networkCfg)
		prov.WriteNodeConfig(t, pipelineNode, pipelineCfg)
		return extKeys
	}

	mfgKeys := writeOrg("mfg", "manufacturer", mfgRegistry, lotProcessDID, mfgLoops(), "",
		[]string{lotPipelineDID}, []string{lotProcessDID})
	distKeys := writeOrg("dist", "distributor", distRegistry, distProcessDID, distLoops("http://mfg-network:8443"), harness.FastTunables,
		[]string{distPipelineDID}, []string{distProcessDID})
	retailKeys := writeOrg("retail", "retailer", retailRegistry, retailNodeProcessDID, retailLoops("http://dist-network:8443"), harness.FastTunables,
		[]string{retailNodePipelineDID}, []string{retailNodeProcessDID})

	c := harness.ComposeUp(t, scenarioDir)
	natsMon := c.Port(t, "nats", 8222)
	harness.WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)

	// waitOrg waits each org's network then pipeline healthy on ITS OWN
	// /readyz (dependency-aware, StartSeparatedNode's own choice — see its
	// doc), returning the network's host-reachable base URL and the instant
	// its readyz passed (WaitWireauthEpochSettle's anchor, below).
	waitOrg := func(node string) (networkBase string, networkReady time.Time) {
		networkNode, pipelineNode := node+"-network", node+"-pipeline"
		networkBase = "http://" + c.Port(t, networkNode, 8443)
		harness.WaitHTTPHealthy(t, networkNode, networkBase+"/readyz", 60*time.Second)
		networkReady = time.Now()
		pipelineBase := "http://" + c.Port(t, pipelineNode, 8443)
		harness.WaitHTTPHealthy(t, pipelineNode, pipelineBase+"/readyz", 60*time.Second)
		return networkBase, networkReady
	}
	mfgBase, mfgReady := waitOrg("mfg")
	distBase, distReady := waitOrg("dist")
	retailBase, retailReady := waitOrg("retail")

	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, ingressSubject, 60*time.Second)
	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, lotPipelineDID, 60*time.Second)
	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, distPipelineDID, 60*time.Second)

	// Each org's own network process carries its own wireauth restart epoch
	// (WaitWireauthEpochSettle's doc); calling it once per org in sequence
	// converges on the single latest-binding org without double-counting
	// (its own doc explains why chained calls compose correctly).
	harness.WaitWireauthEpochSettle(mfgReady)
	harness.WaitWireauthEpochSettle(distReady)
	harness.WaitWireauthEpochSettle(retailReady)

	// Operator bootstrap over the external-key path: every producing org
	// registers on ITS OWN node, so its signing keys live only in its own
	// keystore (KMS model per org), and the private halves never leave the
	// PIPELINE's own local keystore (BootstrapExternal's doc).
	mfgOwner := harness.NewOwner(t, mfgOwnerDID)
	harness.BootstrapExternal(t, mfgBase, mfgOwner, []string{lotPipelineDID}, []string{lotProcessDID}, mfgKeys)
	distOwner := harness.NewOwner(t, distOwnerDID)
	harness.BootstrapExternal(t, distBase, distOwner, []string{distPipelineDID}, []string{distProcessDID}, distKeys)
	retailOwner := harness.NewOwner(t, retailOwnerDID)
	harness.BootstrapExternal(t, retailBase, retailOwner, []string{retailNodePipelineDID}, []string{retailNodeProcessDID}, retailKeys)

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
		retailSink: func() []string { return c.SinkLines(t, "retail-pipeline") },
		registryURL: map[string]string{
			mfgRegistry:    mfgBase,
			distRegistry:   distBase,
			retailRegistry: retailBase,
		},
	}
}
