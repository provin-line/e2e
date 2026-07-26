// Scenario losswindow: "did every reading arrive?" — loss accounting over
// the emission log (E2E-F-027 in FINDINGS.md — the last of the product-surface
// findings; this scenario's own compose-twin gap was E2E-F-028, and its
// delivery waits still carry E2E-F-029).
//
// Story: core NATS is at-most-once. The consumer org's node goes down for a
// window; the producer keeps emitting into the void. Nothing redelivers.
// Before the tlog exposure, that loss was SILENT — the consumer had no way
// to learn it missed anything, let alone what. Now the producer's loop
// appends {credentialHash, sequenceNo} to a durable, checkpoint-signed
// emission log served over dplaax.tlog.v1.TlogService, and the consumer
// reconciles: the signed checkpoint proves HOW MANY events were emitted,
// the record range names WHICH sequence numbers never arrived.
//
// The scenario also pins the two properties the tlog slice hardened:
//   - durability: the producer node RESTARTS and its checkpoint still
//     covers everything emitted before the restart (same head);
//   - sequence continuity: the first post-restart emission carries the
//     NEXT sequence number, not a fork back to 1 (the emitter self-seeds
//     from the durable log tail).
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose). The compose
// twin is not a formality here: the checkpoint is served by the NETWORK
// process from a MIRRORED copy while the source log lives in the PIPELINE
// process, so durability and sequence continuity across the producer restart
// straddle a container boundary the process runtime never crosses. Only the
// consumer-outage half rests on plain stop/start. E2E-F-028 in FINDINGS.md
// records why this file was absent for a while.
package losswindow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/provin-line/oss/canon/jcs"
	"github.com/provin-line/oss/crypto/ed25519"
	"github.com/provin-line/oss/did"
	tlogpb "github.com/provin-line/oss/gen/go/dplaax/tlog/v1"
	"github.com/provin-line/oss/gen/go/dplaax/tlog/v1/tlogpbconnect"
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	mfgRegistry    = "mfg.poc.dplaax.dev"
	retailRegistry = "retail.poc.dplaax.dev"

	mfgOwnerDID    = "did:dplaax:mfg.poc.dplaax.dev:org:mfg"
	retailOwnerDID = "did:dplaax:retail.poc.dplaax.dev:org:retail"

	lotPipelineDID = "did:dplaax:mfg.poc.dplaax.dev:org:mfg:pipeline:lots"
	lotProcessDID  = "did:dplaax:mfg.poc.dplaax.dev:org:mfg:pipeline:lots:process:s1"

	// retail runs no producing/chained loop of its own (only a sink), so it
	// has no natural issuer DID to double as its node identity the way every
	// single-node scenario's NodeDID reuses a loop's process DID (see
	// SingleNodeSpec's doc) — cmd/pipeline's own wireauth-signed
	// RegisterAuditHead call (every consumed head, including a sink's) still
	// needs ONE, so retail gets a dedicated pipeline+process pair for it.
	retailNodePipelineDID = "did:dplaax:retail.poc.dplaax.dev:org:retail:pipeline:node"
	retailNodeProcessDID  = retailNodePipelineDID + ":process:n1"

	ingressSubject = "ingest.lots"
)

func mfgLoops() string {
	return fmt.Sprintf(`
      lots {
        role            = "source"
        ingress-subject = %q
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "lots"
        process-id           = "s1"
        transformation-claim = "convert"
      }`, ingressSubject, lotPipelineDID, lotProcessDID, lotProcessDID+"#signing")
}

func retailLoops(mfgBase string) string {
	return fmt.Sprintf(`
      archive {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`, lotPipelineDID, mfgBase)
}

// sinkDelivery is the consumer's sink output read two ways. Reconciliation is
// keyed on the credential's content address; the delivery waits need to know
// WHICH emission arrived, and only the payload says that.
//
// Counting distinct hashes was enough while core NATS was the only transport —
// at-most-once with no redelivery path makes cardinality and identity the same
// thing. They stop being the same the moment a redelivery path exists: a late
// #2 would satisfy a wait meant for #4, and the run would fail much further
// down as a loss-window mismatch, reporting a delivery bug as a loss-accounting
// bug (E2E-F-029).
type sinkDelivery struct {
	hashes   map[string]bool
	readings map[int]bool
}

// parseSink reads the retail sink's NDJSON. Lines without a credential are not
// sink records — the compose runtime's stream is the container's whole stdout —
// and a record whose payload carries no numeric reading is skipped rather than
// fataled, because this runs inside a polling loop where a half-written line is
// ordinary. Nothing is lost by being lenient here: a reading that never parses
// never joins the set, and the exact-set assertions below then fail naming it.
func parseSink(lines []string) sinkDelivery {
	out := sinkDelivery{hashes: map[string]bool{}, readings: map[int]bool{}}
	for _, line := range lines {
		var rec struct {
			Credential string          `json:"credential"`
			Payload    json.RawMessage `json:"payload"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
			continue
		}
		out.hashes[rec.Credential] = true
		var payload struct {
			Reading *int `json:"reading"`
		}
		if json.Unmarshal(rec.Payload, &payload) == nil && payload.Reading != nil {
			out.readings[*payload.Reading] = true
		}
	}
	return out
}

// readingSet renders a delivered-reading set for comparison and for failure
// messages — the same string in both, so a mismatch reads directly.
func readingSet(m map[int]bool) string {
	ns := make([]int, 0, len(m))
	for n := range m {
		ns = append(ns, n)
	}
	sort.Ints(ns)
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

// lwEnv is what the scenario body needs from either runtime.
type lwEnv struct {
	mfgBase string // producer control plane, host-reachable
	natsURL string
	mfgSeed string
	// sinkLines is the consumer's CURRENT sink output. It is not cumulative
	// across a consumer restart in the process runtime (fresh stdout stream)
	// but is in compose (docker logs accumulate) — the scenario body unions it
	// with a pre-stop snapshot rather than depending on either behaviour.
	sinkLines func() []string
	// waitSubscriber blocks until subject has a subscriber on the broker: a
	// real NATS connection in the process runtime, the broker's monitoring
	// endpoint in compose.
	waitSubscriber func(subject string)
	// stopRetail takes the CONSUMER down and leaves it down — the producer
	// emits into the void and core NATS redelivers nothing.
	stopRetail func()
	// startRetail brings the consumer back with its data dir intact.
	startRetail func()
	// restartMfg restarts the PRODUCER's control and data planes against their
	// same data dirs and returns the network base URL VALID AFTER the restart:
	// compose re-allocates ephemeral published ports across a restart, so
	// clients built on the old URL must be rebuilt (RestartService's doc).
	restartMfg func() string
}

func TestLossWindow_EmissionLogNamesTheLoss(t *testing.T) {
	ctx := context.Background()
	var e lwEnv
	if harness.ComposeRuntime() {
		e = setupCompose(t)
	} else {
		e = setupProcess(t)
	}
	mfgBase := e.mfgBase

	conn, err := natstransport.Connect(ctx, natstransport.Config{URL: e.natsURL, AccountSeed: e.mfgSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	publish := func(n int) {
		t.Helper()
		if err := conn.Publisher(ingressSubject).Publish([]byte(fmt.Sprintf(`{"reading":%d}`, n))); err != nil {
			t.Fatalf("publish #%d: %v", n, err)
		}
	}
	tlogClient := tlogpbconnect.NewTlogServiceClient(http.DefaultClient, mfgBase)
	waitLogSize := func(want int) {
		t.Helper()
		harness.WaitFor(t, fmt.Sprintf("emission log size %d", want), 30*time.Second, func() bool {
			cp, err := tlogClient.GetLogCheckpoint(ctx, harness.Bearer(connect.NewRequest(&tlogpb.GetLogCheckpointRequest{LogId: lotPipelineDID})))
			return err == nil && cp.Msg.GetSize() == fmt.Sprint(want)
		})
	}

	// preStopLines latches what the sink had emitted before the consumer went
	// down. Everything below counts DISTINCT delivered credentials over the
	// union of that snapshot and the consumer's current output, because the two
	// runtimes disagree about what "current output" spans: the process runtime
	// gives the restarted consumer a fresh stdout stream, compose keeps
	// accumulating the same container's docker logs. A bare "at least one
	// delivered" check would therefore be satisfied under compose by #1's line
	// still being there, and would never observe recovery at all.
	var preStopLines []string
	delivered := func() sinkDelivery {
		// Copy first: appending onto preStopLines directly could write through
		// its backing array.
		all := append(append([]string{}, preStopLines...), e.sinkLines()...)
		return parseSink(all)
	}
	// waitDelivered blocks until EXACTLY the named readings have arrived, and
	// says which ones did if they never do — harness.WaitFor can only report the
	// label it was handed, and "timed out waiting for #4" is no help when the
	// interesting fact is that #2 showed up instead.
	waitDelivered := func(what string, want ...int) {
		t.Helper()
		wantSet := map[int]bool{}
		for _, n := range want {
			wantSet[n] = true
		}
		wantKey, got := readingSet(wantSet), ""
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if got = readingSet(delivered().readings); got == wantKey {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("%s: delivered readings = {%s}, want {%s}", what, got, wantKey)
	}

	// --- #1 delivered normally. ---
	publish(1)
	waitDelivered("sink delivery #1", 1)

	// --- The consumer goes DOWN; the producer keeps emitting (#2, #3).
	// Core NATS is at-most-once: nothing will redeliver these. ---
	preStopLines = e.sinkLines()
	e.stopRetail()
	publish(2)
	publish(3)
	waitLogSize(3)

	// --- The consumer recovers; #4 flows again. ---
	e.startRetail()
	e.waitSubscriber(lotPipelineDID)
	publish(4)
	waitDelivered("sink delivery #4 after recovery", 1, 4)

	// --- Durability + sequence continuity: the PRODUCER restarts too. Its
	// log must still cover 1..4, and the next emission must carry sequence
	// 5, not fork back to 1 (the emitter self-seeds from the durable tail).
	//
	// GetLogCheckpoint is served by mfg's NETWORK process from its MIRRORED
	// copy of the log — cmd/pipeline keeps the tlog locally and a mirror
	// shipper ships segments to cmd/network asynchronously (separated
	// topology only; the all-in-one binary served both reads and writes from
	// the same in-process log, no lag possible). "Sink delivery #4" above
	// only proves record 4 reached retail over NATS — NOT that mfg's mirror
	// shipper has shipped it yet — so cpBefore must wait for size 4 itself,
	// or it can race the shipper and capture a stale size-3 checkpoint that
	// then legitimately grows to 4 across the restart below, failing the
	// "restart changes nothing" assertion for a reason that has nothing to
	// do with restart durability. ---
	waitLogSize(4)
	cpBefore, err := tlogClient.GetLogCheckpoint(ctx, harness.Bearer(connect.NewRequest(&tlogpb.GetLogCheckpointRequest{LogId: lotPipelineDID})))
	if err != nil {
		t.Fatalf("GetLogCheckpoint before producer restart: %v", err)
	}
	mfgBase = e.restartMfg()
	// Every client built on the pre-restart base URL is stale under compose.
	tlogClient = tlogpbconnect.NewTlogServiceClient(http.DefaultClient, mfgBase)
	cpAfter, err := tlogClient.GetLogCheckpoint(ctx, harness.Bearer(connect.NewRequest(&tlogpb.GetLogCheckpointRequest{LogId: lotPipelineDID})))
	if err != nil {
		t.Fatalf("GetLogCheckpoint after producer restart: %v", err)
	}
	if cpAfter.Msg.GetSize() != cpBefore.Msg.GetSize() || cpAfter.Msg.GetHead() != cpBefore.Msg.GetHead() {
		t.Fatalf("restart changed the log: before=%s/%s after=%s/%s — emission evidence must be durable",
			cpBefore.Msg.GetSize(), cpBefore.Msg.GetHead(), cpAfter.Msg.GetSize(), cpAfter.Msg.GetHead())
	}
	publish(5)
	waitLogSize(5)
	// Waiting for the LOG to reach 5 does not mean #5 reached the consumer:
	// the mirror shipper and NATS delivery are independent async paths, and
	// reconciling early would count #5 as lost.
	waitDelivered("sink delivery #5 after producer restart", 1, 4, 5)

	// --- Reconciliation: the signed checkpoint proves how many, the record
	// range names WHICH sequence numbers were lost. ---
	cp, err := tlogClient.GetLogCheckpoint(ctx, harness.Bearer(connect.NewRequest(&tlogpb.GetLogCheckpointRequest{LogId: lotPipelineDID})))
	if err != nil {
		t.Fatalf("GetLogCheckpoint: %v", err)
	}
	verifyCheckpointSignature(t, cp.Msg, mfgBase)

	recs, err := tlogClient.ListLogRecords(ctx, harness.Bearer(connect.NewRequest(&tlogpb.ListLogRecordsRequest{LogId: lotPipelineDID})))
	if err != nil {
		t.Fatalf("ListLogRecords: %v", err)
	}
	if len(recs.Msg.GetRecords()) != 5 {
		t.Fatalf("log records = %d, want 5", len(recs.Msg.GetRecords()))
	}

	payloads := make([][]byte, 0, len(recs.Msg.GetRecords()))
	for _, rec := range recs.Msg.GetRecords() {
		payloads = append(payloads, rec.GetPayload())
	}
	recon, err := harness.ReconcileEmissionLog(payloads, delivered().hashes)
	if err != nil {
		t.Fatalf("reconcile emission log: %v", err)
	}
	// Sequence continuity, asserted DIRECTLY on the sequence space: the
	// post-restart emission must carry 5. The loss set cannot show this —
	// record 5 was delivered, so an emitter that forked back to 1 would still
	// leave the loss set at exactly [2 3], indistinguishable from a healthy
	// log. Continuity and loss are separate properties and fail separately.
	if got, want := strings.Join(recon.Sequences, ","), "1,2,3,4,5"; got != want {
		t.Errorf("emission sequences = [%s], want [%s] — the emitter must self-seed from the durable log tail, never fork back to 1", got, want)
	}
	// Every delivered credential is accounted for in the log…
	if len(recon.Unlogged) > 0 {
		t.Errorf("delivered credentials missing from the emission log: %v", recon.Unlogged)
	}
	// …and the loss window is named exactly: sequences 2 and 3.
	if got, want := strings.Join(recon.Lost, ","), "2,3"; got != want {
		t.Fatalf("lost sequences = [%s], want [%s] — the loss window must be nameable", got, want)
	}
}

// verifyCheckpointSignature reconstructs the domain-separated JCS view from
// the response's public fields, resolves signed_by's key from the producer's
// PUBLIC /did/ route, and verifies the signature — the relying party's check
// that the operator is non-repudiably committed to this log head.
func verifyCheckpointSignature(t *testing.T, cp *tlogpb.GetLogCheckpointResponse, mfgBase string) {
	t.Helper()
	if cp.GetLogId() != lotPipelineDID || cp.GetSignedBy() != lotProcessDID+"#signing" {
		t.Fatalf("checkpoint identity = %s / %s, want %s / %s#signing", cp.GetLogId(), cp.GetSignedBy(), lotPipelineDID, lotProcessDID)
	}
	view, err := jcs.Canonicalize(map[string]any{
		"v":         1,
		"purpose":   "dplaax-tlog-checkpoint",
		"logId":     cp.GetLogId(),
		"head":      cp.GetHead(),
		"signedBy":  cp.GetSignedBy(),
		"size":      cp.GetSize(),
		"timestamp": cp.GetTimestamp(),
	})
	if err != nil {
		t.Fatal(err)
	}
	docDID := strings.TrimSuffix(cp.GetSignedBy(), "#signing")
	rest := strings.TrimPrefix(docDID, "did:dplaax:"+mfgRegistry+":")
	resp, err := http.Get(mfgBase + "/did/" + strings.ReplaceAll(rest, ":", "/") + "/did.json")
	if err != nil {
		t.Fatalf("GET signer doc: %v", err)
	}
	raw, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("signer doc: status %d, err %v", resp.StatusCode, readErr)
	}
	var doc did.DIDDocument
	if err := doc.UnmarshalJSON(raw); err != nil {
		t.Fatalf("parse signer doc: %v", err)
	}
	pub, err := did.ExtractPublicKey(&doc, cp.GetSignedBy(), did.RelationshipAssertionMethod)
	if err != nil {
		t.Fatalf("extract signer key: %v", err)
	}
	ok, err := (ed25519.Verifier{}).Verify(pub, view, cp.GetSignature())
	if err != nil || !ok {
		t.Fatalf("checkpoint signature does not verify (ok=%v err=%v) — the head commitment must be non-repudiable", ok, err)
	}
}

// setupProcess boots the two-org topology as local subprocesses: one broker,
// one PDP stub, mfg (producer) and retail (sink), each a cmd/network +
// cmd/pipeline pair against its own data dirs.
func setupProcess(t *testing.T) lwEnv {
	networkBin, pipelineBin := harness.BuildBinaries(t)
	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "mfg", "retail")
	mfgAcc := broker.Account(t, "mfg")
	retailAcc := broker.Account(t, "retail")
	broker.Grant(t, "mfg", "retail", lotPipelineDID)

	mfgNetworkListen, mfgPipelineListen := harness.FreePort(t), harness.FreePort(t)
	retailNetworkListen, retailPipelineListen := harness.FreePort(t), harness.FreePort(t)
	mfgBase := "http://127.0.0.1" + mfgNetworkListen
	retailBase := "http://127.0.0.1" + retailNetworkListen
	regURLs := map[string]string{mfgRegistry: mfgBase, retailRegistry: retailBase}
	pdp := harness.StartPDPStub(t, harness.FreePort(t))

	// newOrgNode prepares (but does not yet start) one org's separated pair:
	// it mints and locally provisions every DID in pipelines then processes
	// (ProvisionExternalIdentity, into the PIPELINE's own data dir) and
	// renders both configs (SplitNodeConfig) ONCE — boot restarts the SAME
	// processes/dirs/configs, never re-provisioning or re-registering
	// (IssuePipeline/IssueProcess is NOT idempotent like RegisterOwner, so a
	// second Bootstrap call over an already-issued DID would fail
	// AlreadyExists). bootstrap (also returned) registers everything over the
	// wire exactly once, called only after the FIRST boot.
	//
	// Pipelines MUST be provisioned before processes (never a map — Go's
	// randomized range order would provision them in either order): a process
	// DID's own resource path structurally nests under its pipeline's
	// (":pipeline:x:process:y" extends ":pipeline:x"), and filestore's
	// DID->directory mapping (didDir, keystore/filestore/filestore.go) joins
	// EVERY colon-separated DID segment as a nested path component — so
	// provisioning the process first would MkdirAll the pipeline's own
	// directory as a side effect, and the pipeline's later SaveKeyPair would
	// then see it "already exists" and fail closed, even though it holds no
	// real keyset yet (env.go's startSingleNodeProcess sidesteps this the
	// same way — PipelineDIDs range fully before ProcessDIDs).
	newOrgNode := func(name, networkListen, pipelineListen, registryID, ownerDID, nodeDID, tunables, loops string, pipelines, processes []string, acc *harness.NATSAccount) (boot func() *harness.SeparatedNode, bootstrap func(sn *harness.SeparatedNode)) {
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
		boot = func() *harness.SeparatedNode {
			return harness.StartSeparatedNode(t, harness.SeparatedNodeSpec{
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
		}
		bootstrap = func(sn *harness.SeparatedNode) {
			owner := harness.NewOwner(t, ownerDID)
			harness.BootstrapExternal(t, sn.BaseURL, owner, pipelines, processes, extKeys)
		}
		return boot, bootstrap
	}

	bootMfg, bootstrapMfg := newOrgNode("mfg", mfgNetworkListen, mfgPipelineListen, mfgRegistry, mfgOwnerDID, lotProcessDID, "", mfgLoops(),
		[]string{lotPipelineDID}, []string{lotProcessDID}, mfgAcc)
	bootRetail, bootstrapRetail := newOrgNode("retail", retailNetworkListen, retailPipelineListen, retailRegistry, retailOwnerDID, retailNodeProcessDID, harness.FastTunables, retailLoops(mfgBase),
		[]string{retailNodePipelineDID}, []string{retailNodeProcessDID}, retailAcc)

	mfgNode := bootMfg()
	bootstrapMfg(mfgNode)
	retailNode := bootRetail()
	bootstrapRetail(retailNode)

	broker.WaitForSubscriber(t, ingressSubject, 30*time.Second)
	broker.WaitForSubscriber(t, lotPipelineDID, 30*time.Second)

	return lwEnv{
		mfgBase:        mfgBase,
		natsURL:        broker.URL,
		mfgSeed:        mfgAcc.Seed,
		sinkLines:      func() []string { return retailNode.SinkLines() },
		waitSubscriber: func(subject string) { broker.WaitForSubscriber(t, subject, 30*time.Second) },
		stopRetail:     func() { retailNode.Stop(t) },
		startRetail:    func() { retailNode = bootRetail() },
		restartMfg: func() string {
			mfgNode.Stop(t)
			mfgNode = bootMfg()
			// The producer's source loop must be back on the ingress subject
			// before the next publish, or the stimulus lands in the void and
			// the sequence-continuity assertion fails for the wrong reason.
			broker.WaitForSubscriber(t, ingressSubject, 30*time.Second)
			// Fixed ports in this runtime: the base URL is unchanged.
			return mfgBase
		},
	}
}

// setupCompose provisions testdata/ (the grant is added BEFORE the broker
// config renders — compose brokers preload account claims statically) and
// boots the two-org compose topology: nats + pdpstub plus FOUR node services,
// mfg and retail each split into its own -network + -pipeline pair, mirroring
// setupProcess org-for-org (AGENTS.md rule 3).
//
// Cross-org resolution uses each org's own -network service DNS name, never a
// -pipeline's: cmd/pipeline carries no in-process registry and is never a
// resolution target. Bootstrap goes over the external-key path for the same
// reason setupProcess does — a separated pipeline's wireauth-signing identity
// needs its private half in ITS OWN data dir, which a registry-side mint can
// never reach (D9 keystore locality).
func setupCompose(t *testing.T) lwEnv {
	scenarioDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testdata := filepath.Join(scenarioDir, "testdata")
	if err := os.RemoveAll(testdata); err != nil {
		t.Fatal(err)
	}

	prov := harness.ProvisionCompose(t, testdata, "mfg", "retail")
	prov.Grant(t, "mfg", "retail", lotPipelineDID)
	prov.WriteBrokerConfig(t)

	regURLsInNet := map[string]string{
		mfgRegistry:    "http://mfg-network:8443",
		retailRegistry: "http://retail-network:8443",
	}

	// writeOrg is setupProcess's newOrgNode minus the starting: it provisions
	// one org's pipeline-local keys and writes both halves of its split config
	// under testdata for the volume mounts. Pipelines before processes, for the
	// nested-DID-directory reason newOrgNode's own doc spells out.
	writeOrg := func(node, registryID, nodeDID, loops, tunables string, pipelines, processes []string) map[string]harness.ExternalKeys {
		networkNode, pipelineNode := node+"-network", node+"-pipeline"
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
			AccountSeedFile:      "/app/secrets/" + node + "-account.seed",
			TrustSeedFile:        "/app/secrets/operator.seed",
			ResolverDir:          "/app/jwts",
			NodeDID:              nodeDID,
			RegistryBaseURLs:     regURLsInNet,
			AllowPrivateNetworks: true,
			LoopsBlock:           loops,
			Tunables:             tunables,
		})
		prov.WriteNodeConfig(t, networkNode, networkCfg)
		prov.WriteNodeConfig(t, pipelineNode, pipelineCfg)
		return extKeys
	}

	mfgKeys := writeOrg("mfg", mfgRegistry, lotProcessDID, mfgLoops(), "",
		[]string{lotPipelineDID}, []string{lotProcessDID})
	retailKeys := writeOrg("retail", retailRegistry, retailNodeProcessDID, retailLoops("http://mfg-network:8443"), harness.FastTunables,
		[]string{retailNodePipelineDID}, []string{retailNodeProcessDID})

	c := harness.ComposeUp(t, scenarioDir)
	natsMon := c.Port(t, "nats", 8222)
	harness.WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)

	// waitOrg waits network then pipeline healthy on their OWN /readyz
	// (pipeline's wireauth-signed boot calls need a live network peer — the
	// same order StartSeparatedNode uses), returning the network's
	// host-reachable base URL.
	waitOrg := func(node string) string {
		networkNode, pipelineNode := node+"-network", node+"-pipeline"
		networkBase := "http://" + c.Port(t, networkNode, 8443)
		harness.WaitHTTPHealthy(t, networkNode, networkBase+"/readyz", 60*time.Second)
		pipelineBase := "http://" + c.Port(t, pipelineNode, 8443)
		harness.WaitHTTPHealthy(t, pipelineNode, pipelineBase+"/readyz", 60*time.Second)
		return networkBase
	}
	mfgBase := waitOrg("mfg")
	retailBase := waitOrg("retail")

	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, ingressSubject, 60*time.Second)
	harness.WaitForSubscriberHTTP(t, "http://"+natsMon, lotPipelineDID, 60*time.Second)

	// No epoch-settle wait: a wireauth-signed call racing network's fresh
	// restart-epoch boot window is retried (re-signed) by the production
	// client until it clears (oss PR #23).
	mfgOwner := harness.NewOwner(t, mfgOwnerDID)
	harness.BootstrapExternal(t, mfgBase, mfgOwner, []string{lotPipelineDID}, []string{lotProcessDID}, mfgKeys)
	retailOwner := harness.NewOwner(t, retailOwnerDID)
	harness.BootstrapExternal(t, retailBase, retailOwner, []string{retailNodePipelineDID}, []string{retailNodeProcessDID}, retailKeys)

	seed, err := os.ReadFile(filepath.Join(testdata, "mfg-account.seed"))
	if err != nil {
		t.Fatal(err)
	}

	return lwEnv{
		mfgBase:        mfgBase,
		natsURL:        "nats://" + c.Port(t, "nats", 4222),
		mfgSeed:        strings.TrimSpace(string(seed)),
		sinkLines:      func() []string { return c.SinkLines(t, "retail-pipeline") },
		waitSubscriber: func(subject string) { harness.WaitForSubscriberHTTP(t, "http://"+natsMon, subject, 60*time.Second) },
		// Pipeline first, then network — cmd/pipeline's ordered shutdown drains
		// its mirror shippers into the registry before exiting (the order
		// SeparatedNode.Stop uses). retail carries no restart policy, so an
		// explicit stop keeps it down for the whole outage window.
		stopRetail: func() {
			c.StopService(t, "retail-pipeline")
			c.StopService(t, "retail-network")
		},
		// Network first on the way back up, for the boot-order reason above.
		startRetail: func() {
			c.StartService(t, "retail-network")
			harness.WaitHTTPHealthy(t, "retail-network", "http://"+c.Port(t, "retail-network", 8443)+"/readyz", 60*time.Second)
			c.StartService(t, "retail-pipeline")
			harness.WaitHTTPHealthy(t, "retail-pipeline", "http://"+c.Port(t, "retail-pipeline", 8443)+"/readyz", 60*time.Second)
		},
		// Both planes go DOWN together before either comes back, matching the
		// process path's SeparatedNode.Stop + reboot. Restarting them one at a
		// time would be a rolling restart: the pipeline would stay up across
		// the network's restart and would never face a cold registry — which is
		// the durability case E2E-F-028 justifies this twin on, so a rolling
		// restart would leave the twin proving less than the finding claims.
		// Down: pipeline then network (cmd/pipeline's ordered shutdown drains
		// its mirror shippers into the registry first). Up: the reverse.
		restartMfg: func() string {
			c.StopService(t, "mfg-pipeline")
			c.StopService(t, "mfg-network")
			c.StartService(t, "mfg-network")
			newBase := "http://" + c.Port(t, "mfg-network", 8443)
			harness.WaitHTTPHealthy(t, "mfg-network", newBase+"/readyz", 60*time.Second)
			c.StartService(t, "mfg-pipeline")
			harness.WaitHTTPHealthy(t, "mfg-pipeline", "http://"+c.Port(t, "mfg-pipeline", 8443)+"/readyz", 60*time.Second)
			harness.WaitForSubscriberHTTP(t, "http://"+natsMon, ingressSubject, 60*time.Second)
			return newBase
		},
	}
}
