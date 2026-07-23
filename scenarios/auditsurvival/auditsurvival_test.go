// Scenario auditsurvival: the regulator arrives AFTER a node restart.
//
// Story: an org ingests a reading, its sink verifies it, the async audit
// records VERIFIED — then the node restarts (a deploy, a crash, a host
// reboot). The auditor asks the same two questions as before the restart:
// "show me the credential" (ResolveVC) and "what was the verdict"
// (GetAuditStatus) — and gets the same answers.
//
// HISTORY: this scenario began life as the CANARY for the in-memory-evidence
// gap (findings #23): it asserted that a restart ERASES the evidence while
// identity survives, with instructions to flip when persistence landed. The
// evidence-persistence slice landed (file-backed CAS under data-dir/evidence/)
// and the canary fired exactly as designed; the assertions below are the
// flipped, permanent contract:
//
//   - survives: DID resolution (control plane), the ability to ingest and
//     verify NEW readings under the same identities — AND the pre-restart
//     evidence: ResolveVC serves the same credential bytes, GetAuditStatus
//     serves the same VERIFIED verdict.
//
// A regression back to evidence loss fails these assertions loudly.
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose; a compose
// restart keeps the container filesystem, matching the process runtime's
// reused data dir).
package auditsurvival

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	auditpb "github.com/provin-line/oss/gen/go/dplaax/audit/v1"
	"github.com/provin-line/oss/gen/go/dplaax/audit/v1/auditpbconnect"
	didpb "github.com/provin-line/oss/gen/go/dplaax/did/v1"
	"github.com/provin-line/oss/gen/go/dplaax/did/v1/didpbconnect"
	vcpb "github.com/provin-line/oss/gen/go/dplaax/vc/v1"
	"github.com/provin-line/oss/gen/go/dplaax/vc/v1/vcpbconnect"
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"
	"github.com/provin-line/oss/vc"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:readings"
	srcProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:readings:process:s1"

	ingressSubject = "ingest.readings"
)

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
		ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing",
		srcPipelineDID, selfBase)
}

func TestAuditSurvival_EvidenceOutlivesRestart(t *testing.T) {
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    []string{srcPipelineDID},
		ProcessDIDs:     []string{srcProcessDID},
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject, srcPipelineDID},
	})
	ctx := context.Background()

	// --- Before the restart: the full story succeeds. ---
	head := ingestAndAudit(t, e, `{"reading":42}`, float64(42))

	didClient := didpbconnect.NewDIDServiceClient(http.DefaultClient, e.NodeBase)
	if _, err := didClient.ResolveDID(ctx, harness.Bearer(connect.NewRequest(&didpb.ResolveDIDRequest{Did: srcProcessDID}))); err != nil {
		t.Fatalf("pre-restart ResolveDID(%s): %v", srcProcessDID, err)
	}
	// Anchor the survival claim: the evidence MUST be retrievable before the
	// restart, or the post-restart success below would prove nothing.
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	if _, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: head}))); err != nil {
		t.Fatalf("pre-restart ResolveVC(%s): %v", head, err)
	}

	// --- The restart. ---
	// The compose runtime re-publishes ephemeral host ports on restart, so
	// everything below runs against the returned base URL (fresh clients).
	e.NodeBase = e.RestartNode()
	didClient = didpbconnect.NewDIDServiceClient(http.DefaultClient, e.NodeBase)
	vcClient = vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)

	// --- Identity and capability SURVIVE (file-backed control plane). ---
	if _, err := didClient.ResolveDID(ctx, harness.Bearer(connect.NewRequest(&didpb.ResolveDIDRequest{Did: srcProcessDID}))); err != nil {
		t.Fatalf("post-restart ResolveDID(%s): identity should survive a restart, got %v", srcProcessDID, err)
	}
	// The same process signs a NEW reading and the whole story works again —
	// keys survived, loops resubscribed, the audit machinery restarted fresh.
	ingestAndAudit(t, e, `{"reading":43}`, float64(43))

	// --- The pre-restart EVIDENCE SURVIVES (file-backed evidence store). ---
	// The credential resolves to the SAME content (the store recomputes and
	// checks the content address on read), and the audit verdict is still
	// VERIFIED — the auditor's questions get the same answers as before.
	resolved, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: head})))
	if err != nil {
		t.Fatalf("post-restart ResolveVC(%s): evidence must survive a restart, got %v", head, err)
	}
	var survived vc.PipelinePassCredential
	if err := json.Unmarshal(resolved.Msg.GetCredential(), &survived); err != nil {
		t.Fatalf("unmarshal survived credential: %v", err)
	}
	if got, err := survived.Hash(); err != nil || got != head {
		t.Fatalf("survived credential hash = %q (err %v), want %q", got, err, head)
	}
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)
	st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: head})))
	if err != nil {
		t.Fatalf("post-restart GetAuditStatus(%s): the verdict must survive a restart, got %v", head, err)
	}
	if lc := st.Msg.GetLinearChain(); lc == nil || lc.GetConfidence() != auditpb.Confidence_CONFIDENCE_VERIFIED {
		t.Fatalf("post-restart audit verdict = %v, want the pre-restart VERIFIED", st.Msg)
	}
}

// ingestAndAudit publishes one reading, waits for the sink's verified record
// carrying the expected payload value, waits for its audit verdict, and
// returns the head credential hash.
func ingestAndAudit(t *testing.T, e harness.SingleNodeEnv, rawJSON string, wantReading float64) string {
	t.Helper()
	ctx := context.Background()

	conn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher(ingressSubject).Publish([]byte(rawJSON)); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	type sinkRecord struct {
		Credential string          `json:"credential"`
		Confidence string          `json:"confidence"`
		Payload    json.RawMessage `json:"payload"`
	}
	var head string
	harness.WaitFor(t, "sink record for "+rawJSON, 60*time.Second, func() bool {
		for _, line := range e.SinkLines() {
			var rec sinkRecord
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
				continue
			}
			var payload map[string]any
			if json.Unmarshal(rec.Payload, &payload) != nil || payload["reading"] != wantReading {
				continue
			}
			if !strings.EqualFold(rec.Confidence, "verified") {
				t.Fatalf("sink confidence = %q, want verified", rec.Confidence)
			}
			head = rec.Credential
			return true
		}
		return false
	})

	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)
	harness.WaitFor(t, "audit VERIFIED for head "+head, 60*time.Second, func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{
			HeadHash: head,
		})))
		if err != nil {
			return false
		}
		lc := st.Msg.GetLinearChain()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})
	return head
}
