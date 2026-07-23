// Scenario recall: "lot X is contaminated — find every descendant."
//
// Story: a defect is discovered in an origin lot AFTER it has fanned out
// through the pipeline. The investigator holds one downstream credential,
// walks BACKWARD to the contaminated origin (previousCredential is inside
// the signed body), then needs the FORWARD direction: every credential
// derived from that origin, and each one's audit verdict. Before the
// discovery layer this direction did not exist on any API (finding #25) and
// heads could only be learned by scraping sink stdout (finding #26).
//
// This scenario drives the three discovery RPCs over the wire:
//   - VCResolverService.ListSuccessors: the forward step, paged one entry
//     per call to exercise the continuation-token discipline;
//   - AuditService.GetAuditStatus per descendant: the recall verdict;
//   - AuditService.ListAuditStatuses: enumeration — the investigator's
//     starting surface — paged to exhaustion;
//   - AuditService.GetConsumedSources on a LINEAR head: NotFound, pinning
//     "no receipt = no consumed-set coverage on this node" (the coverage
//     semantics the aggregate story builds on).
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose).
package recall

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"connectrpc.com/connect"

	auditpb "github.com/provin-line/oss/gen/go/dplaax/audit/v1"
	"github.com/provin-line/oss/gen/go/dplaax/audit/v1/auditpbconnect"
	vcpb "github.com/provin-line/oss/gen/go/dplaax/vc/v1"
	"github.com/provin-line/oss/gen/go/dplaax/vc/v1/vcpbconnect"
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"
	"github.com/provin-line/oss/vc"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:lots"
	srcProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:lots:process:s1"
	aPipelineDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:brancha"
	aProcessDID    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:brancha:process:a1"
	bPipelineDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:branchb"
	bProcessDID    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:branchb:process:b1"

	ingressSubject = "ingest.lots"
	rawJSON        = `{"lot":"L-2026-07","reading":42}`
)

func chainedBlock(selfBase, name, outPipeline, processDID, mark string) string {
	return fmt.Sprintf(`
      %s {
        role            = "chained"
        ingress-subject = %q
        chained {
          output-subject = %q
          issuer {
            did                 = %q
            key-id              = "signing"
            verification-method = %q
          }
          pipeline-id           = %q
          process-id            = %q
          transformation-claim  = "convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
          converter             = "$merge([$, {'branch': '%s'}])"
        }
      }`, name, srcPipelineDID, outPipeline, processDID, processDID+"#signing",
		mark, mark+"1", selfBase, mark)
}

func sinkBlock(selfBase, name, ingress string) string {
	return fmt.Sprintf(`
      %s {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
        }
      }`, name, ingress, selfBase)
}

func loopsBlock(selfBase string) string {
	src := fmt.Sprintf(`
      src {
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
      }`, ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing")
	return src +
		chainedBlock(selfBase, "brancha", aPipelineDID, aProcessDID, "brancha") +
		chainedBlock(selfBase, "branchb", bPipelineDID, bProcessDID, "branchb") +
		sinkBlock(selfBase, "archive-a", aPipelineDID) +
		sinkBlock(selfBase, "archive-b", bPipelineDID)
}

func TestRecall_ForwardTraversalFromContaminatedOrigin(t *testing.T) {
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    []string{srcPipelineDID, aPipelineDID, bPipelineDID},
		ProcessDIDs:     []string{srcProcessDID, aProcessDID, bProcessDID},
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject},
	})
	ctx := context.Background()

	conn, err := natstransport.Connect(ctx, natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher(ingressSubject).Publish([]byte(rawJSON)); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	// Both branch sinks deliver: two descendant heads of one origin lot.
	type sinkRecord struct {
		Credential string `json:"credential"`
	}
	heads := map[string]bool{}
	harness.WaitFor(t, "two branch sink records", 60*time.Second, func() bool {
		heads = map[string]bool{}
		for _, line := range e.SinkLines() {
			var rec sinkRecord
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Credential != "" {
				heads[rec.Credential] = true
			}
		}
		return len(heads) == 2
	})

	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)

	// --- The investigator holds ONE downstream credential and walks
	// backward to the contaminated origin (the link is in the signed body). ---
	var anyHead string
	for h := range heads {
		anyHead = h
		break
	}
	res, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: anyHead})))
	if err != nil {
		t.Fatalf("ResolveVC(%s): %v", anyHead, err)
	}
	var headCred vc.PipelinePassCredential
	if err := headCred.UnmarshalJSON(res.Msg.GetCredential()); err != nil {
		t.Fatalf("unmarshal head: %v", err)
	}
	contaminated := headCred.PreviousCredential()
	if contaminated == "" {
		t.Fatal("branch head has no previousCredential — expected the origin lot")
	}

	// --- FORWARD: every descendant of the contaminated lot, paged one per
	// call to exercise the continuation-token discipline (#25 flipped). ---
	var descendants []string
	pageToken := ""
	for {
		resp, err := vcClient.ListSuccessors(ctx, harness.Bearer(connect.NewRequest(&vcpb.ListSuccessorsRequest{
			Hash:      contaminated,
			PageSize:  1,
			PageToken: pageToken,
		})))
		if err != nil {
			t.Fatalf("ListSuccessors: %v", err)
		}
		descendants = append(descendants, resp.Msg.GetSuccessors()...)
		pageToken = resp.Msg.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	sort.Strings(descendants)
	want := make([]string, 0, 2)
	for h := range heads {
		want = append(want, h)
	}
	sort.Strings(want)
	if len(descendants) != 2 || descendants[0] != want[0] || descendants[1] != want[1] {
		t.Fatalf("descendants of %s = %v, want the two branch heads %v", contaminated, descendants, want)
	}

	// Descendants are leaves: the walk terminates.
	for _, d := range descendants {
		resp, err := vcClient.ListSuccessors(ctx, harness.Bearer(connect.NewRequest(&vcpb.ListSuccessorsRequest{Hash: d})))
		if err != nil || len(resp.Msg.GetSuccessors()) != 0 {
			t.Fatalf("successors(%s) = %v (err %v), want empty leaf", d, resp.Msg.GetSuccessors(), err)
		}
	}

	// --- Each descendant's recall verdict is served. ---
	for _, d := range descendants {
		harness.WaitFor(t, "audit verdict for "+d, 60*time.Second, func() bool {
			resp, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: d})))
			return err == nil && resp.Msg.GetLinearChain().GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
		})
	}

	// --- Enumeration (#26 flipped): the investigator's starting surface.
	// Paged to exhaustion with size 1; the audited set must include both
	// descendants — no sink-stdout scraping involved. ---
	enumerated := map[string]bool{}
	pageToken = ""
	for {
		resp, err := auditClient.ListAuditStatuses(ctx, harness.Bearer(connect.NewRequest(&auditpb.ListAuditStatusesRequest{
			PageSize:  1,
			PageToken: pageToken,
		})))
		if err != nil {
			t.Fatalf("ListAuditStatuses: %v", err)
		}
		for _, entry := range resp.Msg.GetEntries() {
			if entry.GetDamaged() {
				t.Fatalf("enumeration reports damaged head %s on a healthy node", entry.GetHeadHash())
			}
			enumerated[entry.GetHeadHash()] = true
		}
		pageToken = resp.Msg.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	for _, d := range descendants {
		if !enumerated[d] {
			t.Errorf("audited head %s missing from enumeration %v", d, enumerated)
		}
	}

	// --- Coverage semantics: a LINEAR head has no receipt — NotFound, the
	// signal the aggregate story's consumed-set exposure builds on. ---
	_, err = auditClient.GetConsumedSources(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetConsumedSourcesRequest{HeadHash: anyHead})))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetConsumedSources on a linear head: code = %v, want NotFound", connect.CodeOf(err))
	}
}
