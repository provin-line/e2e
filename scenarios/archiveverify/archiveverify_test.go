// Scenario archiveverify: the auditor verifies AFTER the infrastructure died.
//
// Story: during live operation, a relying party archives what a later audit
// needs. Then the node is gone: decommissioned, the vendor folded, the
// evidence subpoenaed years later. The auditor re-verifies the ENTIRE chain
// from the archive alone — no registry, no broker, no provin service.
//
// This pins provin's strongest survivability property: verification does not
// depend on any live infrastructure. Credentials carry their proofs
// (EdDSA-JCS-2022) and chain links (previousCredential content addresses)
// inside the signed body, so bytes + the archived authority documents are
// sufficient forever.
//
// HISTORY: this scenario originally had to INVENT its archive format — the
// product defined no snapshot/export convention, so the survivability
// property was real but unclaimable in practice (E2E-F-024 in FINDINGS.md),
// and the authority-chain rider (signing keys alone are NOT a sufficient archive;
// the controller walk needs process AND pipeline AND owner documents) was
// discovered by this test failing. The product convention has since landed:
// the audit bundle (`provin bundle export` / `provin bundle verify`), whose
// exporter archives exactly what verification resolves. This scenario now
// runs the REAL CLI end to end: export live over the wire surfaces, kill
// the node, re-verify offline anchored by the head (what data flowed) and
// the bundle digest (who signed it — proofs and documents are outside the
// content address and only the digest covers them), then prove tampering is
// caught.
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose).
package archiveverify

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	vcpb "github.com/provin-line/oss/gen/go/dplaax/vc/v1"
	"github.com/provin-line/oss/gen/go/dplaax/vc/v1/vcpbconnect"
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src"
	srcProcessDID    = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src:process:s1"
	relayPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:relay"
	relayProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:relay:process:r1"

	ingressSubject = "ingest.src"
	rawJSON        = `{"reading":42}`
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
        pipeline-id          = "src"
        process-id           = "s1"
        transformation-claim = "convert"
      }
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
          pipeline-id           = "relay"
          process-id            = "r1"
          transformation-claim  = "convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
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
      }`,
		ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing",
		srcPipelineDID, relayPipelineDID, relayProcessDID, relayProcessDID+"#signing", selfBase,
		relayPipelineDID, selfBase)
}

// provin runs one CLI invocation and returns combined output; ok reflects
// the exit status. The CLI is the product surface a relying party actually
// operates — the scenario asserts on its exit codes and printed contract.
func provin(t *testing.T, cli string, args ...string) (output string, ok bool) {
	t.Helper()
	cmd := exec.Command(cli, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return buf.String(), err == nil
}

func TestArchiveVerify_ChainOutlivesInfrastructure(t *testing.T) {
	cli := harness.BuildProvinCLI(t)
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    []string{srcPipelineDID, relayPipelineDID},
		ProcessDIDs:     []string{srcProcessDID, relayProcessDID},
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject, srcPipelineDID, relayPipelineDID},
	})
	ctx := context.Background()

	// --- Live phase: run the story, learn the head from the sink. ---
	conn, err := natstransport.Connect(ctx, natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Publisher(ingressSubject).Publish([]byte(rawJSON)); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	var head string
	harness.WaitFor(t, "sink record", 60*time.Second, func() bool {
		for _, line := range e.SinkLines() {
			if i := strings.Index(line, `"credential":"`); i >= 0 {
				rest := line[i+len(`"credential":"`):]
				if j := strings.IndexByte(rest, '"'); j > 0 {
					head = rest[:j]
					return true
				}
			}
		}
		return false
	})

	// --- The relying party takes the archive: the PRODUCT's command, over
	// the product's wire surfaces (ResolveVC + the public /did/ route). ---
	dir := filepath.Join(t.TempDir(), "bundle")
	out, ok := provin(t, cli, "bundle", "export",
		"--registry", e.NodeBase,
		"--token", harness.BearerToken,
		"--head", head,
		"--out", dir,
		"--did-base", registryID+"="+e.NodeBase,
		"--allow-loopback",
	)
	if !ok {
		t.Fatalf("bundle export failed:\n%s", out)
	}
	var digest string
	for _, line := range strings.Split(out, "\n") {
		if rest, found := strings.CutPrefix(line, "bundle digest: "); found {
			digest = strings.TrimSpace(rest)
		}
	}
	if digest == "" {
		t.Fatalf("export did not print the bundle digest — the out-of-band anchor is the convention's deliverable:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("exported bundle has no manifest: %v", err)
	}

	// --- The infrastructure dies. ---
	e.StopNode()
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	if _, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: head}))); err == nil {
		t.Fatal("node still serving after StopNode — the offline claim below would be hollow")
	}

	// --- Offline phase: the auditor has the bundle, the head, the digest —
	// and nothing else. The verify command never dials by construction. ---
	out, ok = provin(t, cli, "bundle", "verify",
		"--bundle", dir,
		"--head", head,
		"--digest", digest,
	)
	if !ok {
		t.Fatalf("offline bundle verify failed:\n%s", out)
	}
	if !strings.Contains(out, "overall:             VERIFIED") {
		t.Errorf("verify output missing the VERIFIED verdict:\n%s", out)
	}
	if !strings.Contains(out, "anchors checked:     head=true digest=true") {
		t.Errorf("verify did not confirm both external anchors:\n%s", out)
	}

	// --- Tamper-evidence: one flipped byte anywhere in the archive breaks
	// the digest anchor's coverage (proofs and documents included). ---
	headFile := filepath.Join(dir, "credentials", strings.TrimPrefix(head, "sha256:")+".json")
	raw, err := os.ReadFile(headFile)
	if err != nil {
		t.Fatalf("read archived head credential: %v", err)
	}
	raw[len(raw)/2] ^= 0x01
	if err := os.WriteFile(headFile, raw, 0o644); err != nil {
		t.Fatalf("tamper archived credential: %v", err)
	}
	out, ok = provin(t, cli, "bundle", "verify", "--bundle", dir, "--head", head, "--digest", digest)
	if ok {
		t.Fatalf("verify accepted a tampered bundle:\n%s", out)
	}
}
