// Scenario longchain: a deep serial relay — source → 10 chained hops → sink —
// on one node. Exercises what simple cannot:
//
//   - a long chain-preserving credential lineage (11 credentials head→origin);
//   - the async batch resolver + audit runner assembling and verifying the
//     FULL chain (deep chainwalk), not just the adjacent link;
//   - wire chain traversal: the test walks previousCredential hashes from the
//     sink-consumed head back to the FirstDrop via ResolveVC only, verifying
//     every credential independently and checking the hop ordering — the
//     revived "vcresolver-chain" behavior.
package longchain

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
	orgBase    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:"

	hops           = 10
	ingressSubject = "ingest.deep"

	srcPipelineDID = orgBase + "deep"
	srcProcessDID  = srcPipelineDID + ":process:s1"
)

func hopPipelineDID(i int) string { return fmt.Sprintf("%shop%02d", orgBase, i) }
func hopProcessDID(i int) string  { return hopPipelineDID(i) + fmt.Sprintf(":process:p%02d", i) }

// chainDIDs computes every Pipeline/Process DID the chain's loops sign as —
// src plus hop01..hopN — independent of rendering the loops HOCON block, so
// StartSingleNode can provision/issue them before Loops is ever called (it
// needs the full DID list before it can render config, since key
// provisioning must precede cmd/pipeline's boot).
func chainDIDs() (pipelines, processes []string) {
	pipelines = append(pipelines, srcPipelineDID)
	processes = append(processes, srcProcessDID)
	for i := 1; i <= hops; i++ {
		pipelines = append(pipelines, hopPipelineDID(i))
		processes = append(processes, hopProcessDID(i))
	}
	return pipelines, processes
}

// loopsBlock renders src → hop01..hopN → sink. Hop i consumes hop(i-1)'s
// subject (hop 1 consumes the source pipeline) and stamps {'hop': i}.
func loopsBlock(selfBase string) string {
	srcPipeline := srcPipelineDID
	srcProcess := srcProcessDID

	var b strings.Builder
	fmt.Fprintf(&b, `
      src {
        role            = "source"
        ingress-subject = %q
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "deep"
        process-id           = "s1"
        transformation-claim = "convert"
      }`, ingressSubject, srcPipeline, srcProcess, srcProcess+"#signing")

	prevSubject := srcPipeline
	for i := 1; i <= hops; i++ {
		p, proc := hopPipelineDID(i), hopProcessDID(i)
		fmt.Fprintf(&b, `
      hop%02d {
        role            = "chained"
        ingress-subject = %q
        chained {
          output-subject = %q
          issuer {
            did                 = %q
            key-id              = "signing"
            verification-method = %q
          }
          pipeline-id           = "hop%02d"
          process-id            = "p%02d"
          transformation-claim  = "convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
          converter             = "$merge([$, {'hop': %d}])"
        }
      }`, i, prevSubject, p, proc, proc+"#signing", i, i, selfBase, i)
		prevSubject = p
	}

	fmt.Fprintf(&b, `
      archive {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`, prevSubject, selfBase)
	return b.String()
}

func TestLongChain_DeepAuditAndWireTraversal(t *testing.T) {
	ctx := context.Background()
	pipelines, processes := chainDIDs()
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    pipelines,
		ProcessDIDs:     processes,
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject},
	})

	conn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher(ingressSubject).Publish([]byte(`{"reading":7}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// One record after the full relay; converter stamps prove every hop ran in order.
	var head string
	harness.WaitFor(t, "sink NDJSON record after 10 hops", 90*time.Second, func() bool {
		for _, line := range e.SinkLines() {
			var rec struct {
				Credential string          `json:"credential"`
				Confidence string          `json:"confidence"`
				Payload    json.RawMessage `json:"payload"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
				continue
			}
			if !strings.EqualFold(rec.Confidence, "verified") {
				t.Fatalf("sink record not verified: %s", line)
			}
			var p map[string]any
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				t.Fatalf("payload: %v", err)
			}
			if p["hop"] != float64(hops) {
				t.Fatalf("final payload hop = %v, want %d (hops lost in relay)", p["hop"], hops)
			}
			head = rec.Credential
			return true
		}
		return false
	})

	// Wire chain traversal: head → origin via ResolveVC, verifying each link.
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(string) (string, error) {
		return e.NodeBase, nil
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)

	var issuers []string
	hash := head
	for depth := 0; hash != ""; depth++ {
		if depth > hops+1 {
			t.Fatalf("chain longer than expected: depth %d > %d", depth, hops+1)
		}
		resolved, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: hash})))
		if err != nil {
			t.Fatalf("ResolveVC(%s) at depth %d: %v", hash, depth, err)
		}
		var cred vc.PipelinePassCredential
		if err := json.Unmarshal(resolved.Msg.GetCredential(), &cred); err != nil {
			t.Fatalf("unmarshal at depth %d: %v", depth, err)
		}
		if r, err := verifier.Verify(ctx, &cred); err != nil || r.Overall != vc.ConfidenceVerified {
			t.Fatalf("verify at depth %d: overall=%v err=%v", depth, r, err)
		}
		issuers = append(issuers, cred.Issuer())
		hash = cred.PreviousCredential()
	}
	if len(issuers) != hops+1 {
		t.Fatalf("walked %d credentials, want %d", len(issuers), hops+1)
	}
	// Head is the last hop; origin is the source process.
	if issuers[0] != hopProcessDID(hops) {
		t.Errorf("head issuer = %s, want %s", issuers[0], hopProcessDID(hops))
	}
	if got, want := issuers[len(issuers)-1], orgBase+"deep:process:s1"; got != want {
		t.Errorf("origin issuer = %s, want %s", got, want)
	}

	// Deep async audit: the full 11-credential chain records VERIFIED.
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)
	harness.WaitFor(t, "deep-chain audit VERIFIED", 90*time.Second, func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: head})))
		if err != nil {
			return false
		}
		lc := st.Msg.GetLinearChain()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})
}
