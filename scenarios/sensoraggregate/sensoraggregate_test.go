// Scenario sensoraggregate (real-world use case 2): machine-to-machine
// aggregation with a source commitment. Two sensor feeds are signed as
// FirstDrops by their source loops; an aggregate loop pools both, folds them
// into a manifest, and emits ONE provin:aggregate FirstDrop committing to the
// exact consumed set (Merkle source root over the signed wire forms). The node
// self-audits its own emission (emit-locus receipt → async audit runner) and
// serves the source_commitment verdict over GetAuditStatus.
//
// Wire assertions cover the whole C7 story against a deployed node:
//   - sink NDJSON: the aggregate credential verifies and its manifest payload
//     names the two consumed source credentials;
//   - GetAuditStatus: linear_chain VERIFIED and source_commitment present +
//     VERIFIED (the emit-locus self-audit, served over the wire);
//   - consume-locus: the test independently fetches BOTH source credentials
//     via ResolveVC and recomputes the commitment with the product's
//     Verifier.VerifySourceCommitment — the relying-party check that needs no
//     trust in the emitting node's own verdict.
package sensoraggregate

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
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:plant"
	orgBase    = "did:dplaax:poc.dplaax.dev:org:plant:pipeline:"
)

var (
	sensorAPipeline = orgBase + "sensor-a"
	sensorAProcess  = sensorAPipeline + ":process:sa"
	sensorBPipeline = orgBase + "sensor-b"
	sensorBProcess  = sensorBPipeline + ":process:sb"
	aggPipeline     = orgBase + "site-rollup"
	aggProcess      = aggPipeline + ":process:agg"
)

// manifest is ManifestFold's output payload shape.
type manifest struct {
	Sources []string `json:"sources"`
	Count   int      `json:"count"`
}

func sourceBlock(name, ingress, pipeline, process, pipelineID, processID string) string {
	return fmt.Sprintf(`
      %s {
        role            = "source"
        ingress-subject = %q
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = %q
        process-id           = %q
        transformation-claim = "generate"
      }`, name, ingress, pipeline, process, process+"#signing", pipelineID, processID)
}

func loopsBlock(selfBase string) string {
	base := selfBase
	return sourceBlock("sensor-a", "ingest.sensor-a", sensorAPipeline, sensorAProcess, "sensor-a", "sa") +
		sourceBlock("sensor-b", "ingest.sensor-b", sensorBPipeline, sensorBProcess, "sensor-b", "sb") +
		fmt.Sprintf(`
      rollup {
        role = "aggregate"
        aggregate {
          output-subject = %q
          issuer {
            did                 = %q
            key-id              = "signing"
            verification-method = %q
          }
          pipeline-id           = "site-rollup"
          process-id            = "agg"
          verification-strategy = "adjacent"
          window                = 1s
          ingresses {
            a { subject = %q, upstream-endpoint = %q }
            b { subject = %q, upstream-endpoint = %q }
          }
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
      }`, aggPipeline, aggProcess, aggProcess+"#signing",
			sensorAPipeline, base, sensorBPipeline, base,
			aggPipeline, base)
}

func TestSensorAggregate_SourceCommitmentOverTheWire(t *testing.T) {
	ctx := context.Background()
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "plant",
		RegistryID:      registryID,
		NodeDID:         ownerDID,
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{"ingest.sensor-a", "ingest.sensor-b"},
	})

	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, e.NodeBase, owner,
		[]string{sensorAPipeline, sensorBPipeline, aggPipeline},
		[]string{sensorAProcess, sensorBProcess, aggProcess},
	)

	conn, err := natstransport.Connect(natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()

	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	fetch := func(hash string) *vc.PipelinePassCredential {
		resolved, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: hash})))
		if err != nil {
			t.Fatalf("ResolveVC(%s): %v", hash, err)
		}
		var cred vc.PipelinePassCredential
		if err := json.Unmarshal(resolved.Msg.GetCredential(), &cred); err != nil {
			t.Fatalf("unmarshal %s: %v", hash, err)
		}
		return &cred
	}

	// Two autonomous sensor feeds report one reading each. The aggregate
	// window ticker has been firing since boot with uncontrollable phase, so
	// a tick can legitimately land BETWEEN the two admits and split the pair
	// into two 1-source aggregates. The stimulus therefore retries: a fresh
	// reading pair every ~2.5s until some window folds both sensors together.
	// The accept predicate requires both sensor issuers (not just count==2 —
	// under republish a window could pool two readings from one sensor).
	publishPair := func(attempt int) {
		a := fmt.Sprintf(`{"sensor":"a","temp_c":21.5,"attempt":%d}`, attempt)
		b := fmt.Sprintf(`{"sensor":"b","temp_c":22.1,"attempt":%d}`, attempt)
		if err := conn.Publisher("ingest.sensor-a").Publish([]byte(a)); err != nil {
			t.Fatalf("publish sensor-a: %v", err)
		}
		if err := conn.Publisher("ingest.sensor-b").Publish([]byte(b)); err != nil {
			t.Fatalf("publish sensor-b: %v", err)
		}
	}
	publishPair(0)

	var head string
	var sources []*vc.PipelinePassCredential
	seen := map[string]bool{} // aggregate records already inspected
	attempt := 0
	deadline := time.Now().Add(90 * time.Second)
	for head == "" {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a both-sensor aggregate manifest (%d stimulus attempts)", attempt+1)
		}
		for _, line := range e.SinkLines() {
			var rec struct {
				Credential string          `json:"credential"`
				Confidence string          `json:"confidence"`
				Payload    json.RawMessage `json:"payload"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" || seen[rec.Credential] {
				continue
			}
			var m manifest
			if json.Unmarshal(rec.Payload, &m) != nil || m.Count == 0 {
				continue // not an aggregate manifest record
			}
			seen[rec.Credential] = true
			if !strings.EqualFold(rec.Confidence, "verified") {
				t.Fatalf("aggregate sink record not verified: %s", line)
			}
			if m.Count != 2 || len(m.Sources) != 2 {
				continue // split window — legitimate; keep stimulating
			}
			cands := []*vc.PipelinePassCredential{fetch(m.Sources[0]), fetch(m.Sources[1])}
			issuers := map[string]bool{cands[0].Issuer(): true, cands[1].Issuer(): true}
			if !issuers[sensorAProcess] || !issuers[sensorBProcess] {
				continue // two readings from one sensor pooled together — keep going
			}
			head, sources = rec.Credential, cands
			break
		}
		if head == "" {
			time.Sleep(250 * time.Millisecond)
			attempt++
			if attempt%10 == 0 { // fresh stimulus every ~2.5s
				publishPair(attempt / 10)
			}
		}
	}

	aggCred := fetch(head)
	if aggCred.Issuer() != aggProcess {
		t.Errorf("aggregate issuer = %s, want %s", aggCred.Issuer(), aggProcess)
	}

	// Consume-locus recomputation: the relying-party check over exactly the
	// fetched set. Independent of the emitting node's own audit verdict.
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(string) (string, error) {
		return e.NodeBase, nil
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	if r, err := verifier.Verify(ctx, aggCred); err != nil || r.Overall != vc.ConfidenceVerified {
		t.Fatalf("aggregate credential verify: overall=%v err=%v", r, err)
	}
	state, err := verifier.VerifySourceCommitment(ctx, aggCred, sources)
	if err != nil {
		t.Fatalf("VerifySourceCommitment: %v", err)
	}
	if state != vc.ConfidenceVerified {
		t.Fatalf("consume-locus source commitment = %v, want Verified", state)
	}

	// Emit-locus self-audit served over the wire: linear chain AND source
	// commitment verdicts both VERIFIED for the aggregate head.
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)
	harness.WaitFor(t, "audit linear+source_commitment VERIFIED", 60*time.Second, func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: head})))
		if err != nil {
			return false
		}
		lc, sc := st.Msg.GetLinearChain(), st.Msg.GetSourceCommitment()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED &&
			sc != nil && sc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})
}
