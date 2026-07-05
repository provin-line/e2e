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

func loopsBlock(listenAddr string) string {
	base := "http://127.0.0.1" + listenAddr
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
	bin := harness.BuildStandalone(t)
	listenAddr := harness.FreePort(t)
	pdpURL := harness.StartPDPStub(t, harness.FreePort(t))

	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "plant")
	plant := broker.Account(t, "plant")

	baseURL := "http://127.0.0.1" + listenAddr
	cfg := harness.NodeConfig{
		ListenAddr:      listenAddr,
		RegistryID:      registryID,
		PDPBaseURL:      pdpURL,
		NATSURL:         broker.URL,
		AccountSeedFile: plant.SeedFile,
		TrustSeedFile:   broker.TrustSeedFile,
		ResolverDir:     broker.ResolverDir,
		NodeDID:         ownerDID,
		ResolverBaseURL: baseURL,
		VCStoreEndpoint: baseURL,
		LoopsBlock:      loopsBlock(listenAddr),
		Extra: `    batch-resolver { interval = 1s, batch-size = 64, max-retries = 5, max-depth = 1024 }
    audit-runner { interval = 1s, batch-size = 64, max-attempts = 10 }`,
	}

	nodeDir := filepath.Join(workDir, "plant-node")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	node := harness.StartNode(t, "plant", bin, nodeDir, listenAddr, cfg.Render())

	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, node.BaseURL, owner,
		[]string{sensorAPipeline, sensorBPipeline, aggPipeline},
		[]string{sensorAProcess, sensorBProcess, aggProcess},
	)

	// Two autonomous sensor feeds report one reading each.
	conn, err := natstransport.Connect(natstransport.Config{URL: broker.URL, AccountSeed: plant.Seed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher("ingest.sensor-a").Publish([]byte(`{"sensor":"a","temp_c":21.5}`)); err != nil {
		t.Fatalf("publish sensor-a: %v", err)
	}
	if err := conn.Publisher("ingest.sensor-b").Publish([]byte(`{"sensor":"b","temp_c":22.1}`)); err != nil {
		t.Fatalf("publish sensor-b: %v", err)
	}

	// The window fires and the sink consumes ONE aggregate credential whose
	// manifest names exactly two source credentials.
	var head string
	var manifest struct {
		Sources []string `json:"sources"`
		Count   int      `json:"count"`
	}
	harness.WaitFor(t, "aggregate sink record with 2-source manifest", 60*time.Second, func() bool {
		for _, line := range node.SinkLines() {
			var rec struct {
				Credential string          `json:"credential"`
				Confidence string          `json:"confidence"`
				Payload    json.RawMessage `json:"payload"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
				continue
			}
			var m struct {
				Sources []string `json:"sources"`
				Count   int      `json:"count"`
			}
			if json.Unmarshal(rec.Payload, &m) != nil || m.Count == 0 {
				continue // not the aggregate manifest record
			}
			if !strings.EqualFold(rec.Confidence, "verified") {
				t.Fatalf("aggregate sink record not verified: %s", line)
			}
			if m.Count != 2 || len(m.Sources) != 2 {
				// A window may fire with one pooled input if the second sensor
				// lands late; keep waiting for the 2-source fold.
				continue
			}
			head, manifest = rec.Credential, m
			return true
		}
		return false
	})

	// Fetch the aggregate credential and both consumed sources over the wire.
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, node.BaseURL)
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
	aggCred := fetch(head)
	if aggCred.Issuer() != aggProcess {
		t.Errorf("aggregate issuer = %s, want %s", aggCred.Issuer(), aggProcess)
	}
	sources := make([]*vc.PipelinePassCredential, 0, len(manifest.Sources))
	issuerSeen := map[string]bool{}
	for _, h := range manifest.Sources {
		c := fetch(h)
		issuerSeen[c.Issuer()] = true
		sources = append(sources, c)
	}
	if !issuerSeen[sensorAProcess] || !issuerSeen[sensorBProcess] {
		t.Errorf("consumed sources issuers = %v, want both sensors", issuerSeen)
	}

	// Consume-locus recomputation: the relying-party check over exactly the
	// fetched set. Independent of the emitting node's own audit verdict.
	guard := core.NewURLGuard(core.WithAllowLoopback(true))
	didres := didresolver.New(guard, didresolver.WithRegistryBaseURL(func(string) (string, error) {
		return node.BaseURL, nil
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
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, node.BaseURL)
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
