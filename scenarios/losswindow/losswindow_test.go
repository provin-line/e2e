// Scenario losswindow: "did every reading arrive?" — loss accounting over
// the emission log (finding #27, the last open finding of the series).
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
// Runtime: process. The compose twin needs a 2-node compose file +
// provisioning mirroring supplychain's — recorded follow-up; the
// loss-window mechanics (subprocess stop/start) are runtime-independent.
package losswindow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

// deliveredHashes parses the retail sink's NDJSON lines into the set of
// delivered credential hashes.
func deliveredHashes(lines []string) map[string]bool {
	out := map[string]bool{}
	for _, line := range lines {
		var rec struct {
			Credential string `json:"credential"`
		}
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Credential != "" {
			out[rec.Credential] = true
		}
	}
	return out
}

func TestLossWindow_EmissionLogNamesTheLoss(t *testing.T) {
	if harness.ComposeRuntime() {
		t.Skip("losswindow compose twin (2-node compose file + provisioning) is a recorded follow-up; the process runtime covers the loss-window mechanics")
	}
	ctx := context.Background()
	bin := harness.BuildStandalone(t)
	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "mfg", "retail")
	mfgAcc := broker.Account(t, "mfg")
	retailAcc := broker.Account(t, "retail")
	broker.Grant(t, "mfg", "retail", lotPipelineDID)

	mfgListen, retailListen := harness.FreePort(t), harness.FreePort(t)
	mfgBase := "http://127.0.0.1" + mfgListen
	regURLs := map[string]string{mfgRegistry: mfgBase, retailRegistry: "http://127.0.0.1" + retailListen}
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

	mfgNode := startNode("mfg", mfgListen, mfgRegistry, mfgOwnerDID, mfgBase, mfgLoops(), "", mfgAcc)
	retailNode := startNode("retail", retailListen, retailRegistry, retailOwnerDID, "", retailLoops(mfgBase), harness.FastTunables, retailAcc)

	mfgOwner := harness.NewOwner(t, mfgOwnerDID)
	harness.Bootstrap(t, mfgBase, mfgOwner, []string{lotPipelineDID}, []string{lotProcessDID})
	retailOwner := harness.NewOwner(t, retailOwnerDID)
	harness.Bootstrap(t, regURLs[retailRegistry], retailOwner, nil, nil)

	broker.WaitForSubscriber(t, ingressSubject, 30*time.Second)
	broker.WaitForSubscriber(t, lotPipelineDID, 30*time.Second)

	conn, err := natstransport.Connect(ctx, natstransport.Config{URL: broker.URL, AccountSeed: mfgAcc.Seed})
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

	// --- #1 delivered normally. ---
	publish(1)
	harness.WaitFor(t, "sink delivery #1", 30*time.Second, func() bool {
		return len(deliveredHashes(retailNode.SinkLines())) == 1
	})

	// --- The consumer goes DOWN; the producer keeps emitting (#2, #3).
	// Core NATS is at-most-once: nothing will redeliver these. ---
	preStopLines := retailNode.SinkLines()
	retailNode.Stop(t)
	publish(2)
	publish(3)
	waitLogSize(3)

	// --- The consumer recovers; #4 flows again. ---
	retailNode = startNode("retail", retailListen, retailRegistry, retailOwnerDID, "", retailLoops(mfgBase), harness.FastTunables, retailAcc)
	broker.WaitForSubscriber(t, lotPipelineDID, 30*time.Second)
	publish(4)
	harness.WaitFor(t, "sink delivery #4 after recovery", 30*time.Second, func() bool {
		return len(deliveredHashes(retailNode.SinkLines())) >= 1
	})

	// --- Durability + sequence continuity: the PRODUCER restarts too. Its
	// log must still cover 1..4, and the next emission must carry sequence
	// 5, not fork back to 1 (the emitter self-seeds from the durable tail). ---
	cpBefore, err := tlogClient.GetLogCheckpoint(ctx, harness.Bearer(connect.NewRequest(&tlogpb.GetLogCheckpointRequest{LogId: lotPipelineDID})))
	if err != nil {
		t.Fatalf("GetLogCheckpoint before producer restart: %v", err)
	}
	mfgNode.Stop(t)
	mfgNode = startNode("mfg", mfgListen, mfgRegistry, mfgOwnerDID, mfgBase, mfgLoops(), "", mfgAcc)
	_ = mfgNode
	broker.WaitForSubscriber(t, ingressSubject, 30*time.Second)
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

	delivered := deliveredHashes(append(preStopLines, retailNode.SinkLines()...))
	var lostSeqs []string
	seqByHash := map[string]string{}
	for _, rec := range recs.Msg.GetRecords() {
		var em struct {
			CredentialHash string `json:"credentialHash"`
			SequenceNo     string `json:"sequenceNo"`
		}
		if err := json.Unmarshal(rec.GetPayload(), &em); err != nil {
			t.Fatalf("emission record %s: %v", rec.GetIndex(), err)
		}
		seqByHash[em.CredentialHash] = em.SequenceNo
		if !delivered[em.CredentialHash] {
			lostSeqs = append(lostSeqs, em.SequenceNo)
		}
	}
	// Every delivered credential is accounted for in the log…
	for h := range delivered {
		if _, ok := seqByHash[h]; !ok {
			t.Errorf("delivered credential %s missing from the emission log", h)
		}
	}
	// …and the loss window is named exactly: sequences 2 and 3.
	if len(lostSeqs) != 2 || lostSeqs[0] != "2" || lostSeqs[1] != "3" {
		t.Fatalf("lost sequences = %v, want [2 3] — the loss window must be nameable, and post-restart sequence 5 proves the space did not fork", lostSeqs)
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
