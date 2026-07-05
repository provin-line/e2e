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
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"
	orgBase    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:"

	hops           = 10
	ingressSubject = "ingest.deep"
	listenAddr     = ":18463"
	pdpAddr        = ":19093"
)

func hopPipelineDID(i int) string { return fmt.Sprintf("%shop%02d", orgBase, i) }
func hopProcessDID(i int) string  { return hopPipelineDID(i) + fmt.Sprintf(":process:p%02d", i) }

// loopsBlock renders src → hop01..hopN → sink. Hop i consumes hop(i-1)'s
// subject (hop 1 consumes the source pipeline) and stamps {'hop': i}.
func loopsBlock() (block string, pipelines, processes []string) {
	srcPipeline := orgBase + "deep"
	srcProcess := srcPipeline + ":process:s1"
	pipelines = append(pipelines, srcPipeline)
	processes = append(processes, srcProcess)

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
		pipelines = append(pipelines, p)
		processes = append(processes, proc)
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
          upstream-endpoint     = "http://127.0.0.1%s"
          converter             = "$merge([$, {'hop': %d}])"
        }
      }`, i, prevSubject, p, proc, proc+"#signing", i, i, listenAddr, i)
		prevSubject = p
	}

	fmt.Fprintf(&b, `
      archive {
        role            = "sink"
        ingress-subject = %q
        sink {
          kind                  = "observation-only"
          verification-strategy = "adjacent"
          upstream-endpoint     = "http://127.0.0.1%s"
        }
      }`, prevSubject, listenAddr)
	return b.String(), pipelines, processes
}

func TestLongChain_DeepAuditAndWireTraversal(t *testing.T) {
	ctx := context.Background()
	bin := harness.BuildStandalone(t)
	pdpURL := harness.StartPDPStub(t, pdpAddr)

	workDir := t.TempDir()
	broker := harness.StartNATS(t, filepath.Join(workDir, "nats"), "acme")
	acme := broker.Account(t, "acme")

	baseURL := "http://127.0.0.1" + listenAddr
	loops, pipelines, processes := loopsBlock()
	cfg := harness.NodeConfig{
		ListenAddr:      listenAddr,
		RegistryID:      registryID,
		PDPBaseURL:      pdpURL,
		NATSURL:         broker.URL,
		AccountSeedFile: acme.SeedFile,
		TrustSeedFile:   broker.TrustSeedFile,
		ResolverDir:     broker.ResolverDir,
		NodeDID:         ownerDID,
		ResolverBaseURL: baseURL,
		VCStoreEndpoint: baseURL,
		LoopsBlock:      loops,
		Extra: `    batch-resolver { interval = 1s, batch-size = 64, max-retries = 5, max-depth = 1024 }
    audit-runner { interval = 1s, batch-size = 64, max-attempts = 10 }`,
	}

	nodeDir := filepath.Join(workDir, "acme-node")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	node := harness.StartNode(t, "acme", bin, nodeDir, listenAddr, cfg.Render())

	owner := harness.NewOwner(t, ownerDID)
	harness.Bootstrap(t, node.BaseURL, owner, pipelines, processes)

	conn, err := natstransport.Connect(natstransport.Config{URL: broker.URL, AccountSeed: acme.Seed})
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
		for _, line := range node.SinkLines() {
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
		return node.BaseURL, nil
	}))
	verifier := vc.NewVerifier(didres, ed25519.Verifier{})
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, node.BaseURL)

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
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, node.BaseURL)
	harness.WaitFor(t, "deep-chain audit VERIFIED", 90*time.Second, func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: head})))
		if err != nil {
			return false
		}
		lc := st.Msg.GetLinearChain()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})
}
