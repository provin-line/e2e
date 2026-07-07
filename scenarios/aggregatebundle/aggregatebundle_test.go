// Scenario aggregatebundle: the C7 verdict outlives the infrastructure.
//
// Story: S8 proved the LINEAR chain re-verifies from an archive after the
// node is gone; the differentiating claim — the aggregate's source
// commitment (which sensor readings fed this rollup?) — did not travel.
// The aggregate-complete bundle closes that: `provin bundle export
// --aggregate-complete` fetches the emit-locus receipt, bundles the
// consumed source credentials, and the offline verifier RECOMPUTES the
// signed Merkle root from those sources. "Complete" is complete with
// respect to the SIGNED claimed source set.
//
// This drives the real CLI end to end over an S5-style two-sensor
// aggregate: export with the flag (receipt + sources travel), export
// without it (the linear-only claim stays honest), node killed and
// asserted dead, offline verify → VERIFIED with the source-commitment
// line, and one flipped byte in a bundled SOURCE credential caught through
// the bundle digest.
//
// Runtimes: process (default) and compose (E2E_RUNTIME=compose).
package aggregatebundle

import (
	"bytes"
	"context"
	"encoding/json"
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

func loopsBlock(selfBase string) string {
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
			sensorAPipeline, selfBase, sensorBPipeline, selfBase,
			aggPipeline, selfBase)
}

func provin(t *testing.T, cli string, args ...string) (string, bool) {
	t.Helper()
	cmd := exec.Command(cli, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return buf.String(), err == nil
}

func digestOf(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if rest, ok := strings.CutPrefix(line, "bundle digest: "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatalf("no bundle digest in output:\n%s", out)
	return ""
}

func TestAggregateBundle_SourceCommitmentOutlivesInfrastructure(t *testing.T) {
	cli := harness.BuildProvinCLI(t)
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

	conn, err := natstransport.Connect(ctx, natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()

	// Stimulate until some window folds BOTH sensors (the ticker phase is
	// uncontrollable — same retry discipline as S5).
	publishPair := func(attempt int) {
		for _, s := range []struct{ subj, body string }{
			{"ingest.sensor-a", fmt.Sprintf(`{"sensor":"a","temp_c":21.5,"attempt":%d}`, attempt)},
			{"ingest.sensor-b", fmt.Sprintf(`{"sensor":"b","temp_c":22.1,"attempt":%d}`, attempt)},
		} {
			if err := conn.Publisher(s.subj).Publish([]byte(s.body)); err != nil {
				t.Fatalf("publish %s: %v", s.subj, err)
			}
		}
	}
	publishPair(0)

	var head string
	seen := map[string]bool{}
	attempt := 0
	deadline := time.Now().Add(90 * time.Second)
	for head == "" {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a both-sensor aggregate (%d attempts)", attempt+1)
		}
		for _, line := range e.SinkLines() {
			var rec struct {
				Credential string          `json:"credential"`
				Payload    json.RawMessage `json:"payload"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" || seen[rec.Credential] {
				continue
			}
			var m struct {
				Sources []string `json:"sources"`
				Count   int      `json:"count"`
			}
			if json.Unmarshal(rec.Payload, &m) != nil || m.Count != 2 {
				continue
			}
			seen[rec.Credential] = true
			head = rec.Credential
			break
		}
		if head == "" {
			time.Sleep(250 * time.Millisecond)
			attempt++
			if attempt%10 == 0 {
				publishPair(attempt / 10)
			}
		}
	}

	// --- Export BOTH claims while the node lives: the aggregate-complete
	// bundle (receipt + sources travel) and the plain linear one (whose
	// report must keep claiming only linear coverage — no scope inflation). ---
	aggDir := filepath.Join(t.TempDir(), "agg-bundle")
	exportArgs := []string{"bundle", "export",
		"--registry", e.NodeBase, "--token", harness.BearerToken,
		"--head", head, "--out", aggDir,
		"--did-base", registryID + "=" + e.NodeBase,
		"--allow-loopback", "--aggregate-complete",
	}
	if harness.ComposeRuntime() {
		// Split-horizon: the container-network selfBase the node advertises
		// as #vc-resolver (http://plant:8443) is unreachable from the host —
		// exactly the deployment shape --vc-resolver-base exists for.
		exportArgs = append(exportArgs, "--vc-resolver-base", registryID+"="+e.NodeBase)
	}
	out, ok := provin(t, cli, exportArgs...)
	if !ok {
		t.Fatalf("aggregate-complete export failed:\n%s", out)
	}
	if !strings.Contains(out, "scope:          aggregate-complete") {
		t.Fatalf("export output missing the aggregate-complete scope:\n%s", out)
	}
	aggDigest := digestOf(t, out)

	linDir := filepath.Join(t.TempDir(), "lin-bundle")
	out, ok = provin(t, cli, "bundle", "export",
		"--registry", e.NodeBase, "--token", harness.BearerToken,
		"--head", head, "--out", linDir,
		"--did-base", registryID+"="+e.NodeBase,
		"--allow-loopback",
	)
	if !ok {
		t.Fatalf("linear export failed:\n%s", out)
	}
	linDigest := digestOf(t, out)

	// --- The infrastructure dies. ---
	e.StopNode()
	vcClient := vcpbconnect.NewVCResolverServiceClient(http.DefaultClient, e.NodeBase)
	if _, err := vcClient.ResolveVC(ctx, harness.Bearer(connect.NewRequest(&vcpb.ResolveVCRequest{Hash: head}))); err == nil {
		t.Fatal("node still serving after StopNode — the offline claims below would be hollow")
	}

	// --- Offline: the full C7 verdict from the aggregate-complete bundle. ---
	out, ok = provin(t, cli, "bundle", "verify", "--bundle", aggDir, "--head", head, "--digest", aggDigest)
	if !ok {
		t.Fatalf("offline aggregate-complete verify failed:\n%s", out)
	}
	for _, want := range []string{
		"scope:               aggregate-complete",
		"source commitments:  1 over 2 bundled source(s)",
		"overall:             VERIFIED",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("verify output missing %q:\n%s", want, out)
		}
	}

	// --- The linear bundle stays honest: VERIFIED, but claiming only the
	// spine (scope linear, no source-commitment line). ---
	out, ok = provin(t, cli, "bundle", "verify", "--bundle", linDir, "--head", head, "--digest", linDigest)
	if !ok {
		t.Fatalf("offline linear verify failed:\n%s", out)
	}
	if !strings.Contains(out, "scope:               linear") || strings.Contains(out, "source commitments:") {
		t.Fatalf("linear bundle's claim inflated:\n%s", out)
	}

	// --- Tamper-evidence reaches the SOURCES: flip one byte in a bundled
	// source credential (not on the main spine) — the digest coverage
	// catches it. ---
	var sourceFile string
	entries, err := os.ReadDir(filepath.Join(aggDir, "credentials"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 { // aggregate head + two sources
		t.Fatalf("aggregate bundle holds %d credentials, want 3", len(entries))
	}
	for _, ent := range entries {
		if ent.Name() != strings.TrimPrefix(head, "sha256:")+".json" {
			sourceFile = filepath.Join(aggDir, "credentials", ent.Name())
			break
		}
	}
	raw, err := os.ReadFile(sourceFile)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0x01
	if err := os.WriteFile(sourceFile, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	out, ok = provin(t, cli, "bundle", "verify", "--bundle", aggDir, "--head", head, "--digest", aggDigest)
	if ok {
		t.Fatalf("verify accepted a bundle with a tampered SOURCE credential:\n%s", out)
	}
}
