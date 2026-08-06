//go:build demo

// TestDemo narrates this scenario's tamper arc for provin.dev's landing
// page: THIS OUTPUT IS THE PRODUCT, not a test log. It reuses setupCompose —
// the exact compose-runtime bootstrap TestSupplyChain_ThreeOrgsOwnRegistries
// itself boots — so the demo can never drift from what the assertion-grade
// test in supplychain_test.go actually proves; nothing about the topology is
// re-derived here. What follows setup is a deliberately thinner slice of the
// same arc: the benign delivery, the payload-substitution tamper probe, and
// (new to the demo) an offline bundle re-verify. The strict-intake
// quarantine and cross-org chain-walk audit are assertion-grade detail this
// narration does not need — see supplychain_test.go for the full picture.
//
// Narration goes through fmt.Println/Printf directly, bypassing the testing
// package's per-test log buffering, so it streams in real time and stays
// clean of go test's own scaffolding when run as a compiled test binary
// (`go test -tags demo -c` + `./demo.test -test.run ^TestDemo$`, see the
// Makefile's `demo` target). t itself drives only fail-closed control flow
// (t.Fatalf) — never expected to fire against a healthy stack, but the
// harness underneath is the same real one the assertion-grade test uses, so
// a genuine break surfaces here exactly as it would there.
package supplychain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/provin-line/oss/agentaccess"
	"github.com/provin-line/oss/appraisal"
	"github.com/provin-line/oss/pipeline/transport/envelopecodec"
	natstransport "github.com/provin-line/oss/pipeline/transport/nats"

	"github.com/provin-line/e2e/internal/harness"
)

func TestDemo(t *testing.T) {
	start := time.Now()
	ctx := context.Background()
	fmt.Println("provin tamper demo — three organizations, one forged payload")

	fmt.Println("[1/6] starting three organizations (manufacturer / distributor / retailer), each with its own registry …")
	e := setupCompose(t)

	mfgConn, err := natstransport.Connect(ctx, natstransport.Config{URL: e.natsURL, AccountSeed: e.mfgSeed})
	if err != nil {
		t.Fatalf("manufacturer connect: %v", err)
	}
	defer mfgConn.Close()
	sourceWire := make(chan []byte, 4)
	if err := mfgConn.Subscriber(lotPipelineDID).Subscribe(func(b []byte) {
		sourceWire <- append([]byte(nil), b...)
	}); err != nil {
		t.Fatalf("observe manufacturer output: %v", err)
	}

	const lot = "LOT-2026-08-06"
	lotPayload := []byte(fmt.Sprintf(`{"lot":%q,"co2e_kg":12.5,"site":"osaka-plant-1"}`, lot))
	if err := mfgConn.Publisher(ingressSubject).Publish(lotPayload); err != nil {
		t.Fatalf("publish lot record: %v", err)
	}

	var captured []byte
	select {
	case captured = <-sourceWire:
	case <-time.After(30 * time.Second):
		t.Fatal("manufacturer signed envelope was not observable for the tamper probe")
	}
	envelope, err := envelopecodec.New().UnmarshalEnvelope(captured)
	if err != nil {
		t.Fatalf("decode manufacturer envelope: %v", err)
	}
	mfgCredHash, err := envelope.Credential.Hash()
	if err != nil {
		t.Fatalf("hash manufacturer credential: %v", err)
	}
	fmt.Printf("[2/6] manufacturer ingests lot %s: signed FirstDrop credential issued (%s)\n", lot, envelope.Credential.Issuer())
	fmt.Printf("      credential hash %s\n", mfgCredHash)

	var head string
	harness.WaitFor(t, "retailer sink record", 60*time.Second, func() bool {
		for _, line := range e.retailSink() {
			var rec struct {
				Credential   string                      `json:"credential"`
				Confidence   string                      `json:"confidence"`
				Payload      json.RawMessage             `json:"payload"`
				EvidenceView *appraisal.View             `json:"evidenceView"`
				Delivery     *agentaccess.DeliveryRecord `json:"delivery"`
			}
			if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
				continue
			}
			var p map[string]any
			if json.Unmarshal(rec.Payload, &p) != nil || p["lot"] != lot {
				continue
			}
			if !strings.EqualFold(rec.Confidence, "verified") || rec.EvidenceView == nil ||
				rec.EvidenceView.PolicyDecision == nil || rec.EvidenceView.PolicyDecision.Decision != appraisal.DecisionAccept ||
				rec.Delivery == nil {
				t.Fatalf("retailer sink record not cleanly accepted: %s", line)
			}
			head = rec.Credential
			return true
		}
		return false
	})
	fmt.Println("[3/6] distributor transforms and delivers → retailer verifies the chain and \033[1mACCEPTS\033[0m")
	acceptedBeforeTamper := len(e.retailSink())
	fmt.Printf("      retailer delivery records: %d (bound to verified evidence view %s)\n", acceptedBeforeTamper, head)

	fmt.Println("[4/6] adversary republishes the manufacturer envelope with a FORGED payload (lot swapped)")
	envelope.Payload = []byte(fmt.Sprintf(`{"lot":%q,"co2e_kg":0,"site":"attacker-substitution"}`, lot))
	tamperedWire, err := envelopecodec.New().MarshalEnvelope(envelope)
	if err != nil {
		t.Fatalf("encode tampered envelope: %v", err)
	}
	if err := mfgConn.Publisher(lotPipelineDID).Publish(tamperedWire); err != nil {
		t.Fatalf("publish tampered envelope: %v", err)
	}
	time.Sleep(2 * time.Second)

	fmt.Println("[5/6] relay \033[1mHALTS\033[0m at the next boundary — signature no longer matches the recorded output")
	after := len(e.retailSink())
	if after != acceptedBeforeTamper {
		t.Fatalf("forged payload reached delivery: before=%d after=%d", acceptedBeforeTamper, after)
	}
	fmt.Printf("      retailer delivery records: %d (unchanged — the forged payload never arrived)\n", after)

	cli := harness.BuildProvinCLI(t)
	dir := filepath.Join(t.TempDir(), "bundle")
	out, ok := runProvin(t, cli, "bundle", "export",
		"--registry", e.retailBase,
		"--token", harness.BearerToken,
		"--head", head,
		"--out", dir,
		"--did-base", mfgRegistry+"="+e.mfgBase,
		"--did-base", distRegistry+"="+e.distBase,
		"--did-base", retailRegistry+"="+e.retailBase,
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
		t.Fatalf("bundle export did not print a digest:\n%s", out)
	}
	out, ok = runProvin(t, cli, "bundle", "verify", "--bundle", dir, "--head", head, "--digest", digest)
	if !ok || !strings.Contains(out, "overall:             VERIFIED") {
		t.Fatalf("offline bundle verify did not confirm VERIFIED:\n%s", out)
	}
	fmt.Println("[6/6] evidence survives: exported bundle re-verifies offline → \033[1mPASS\033[0m")

	fmt.Printf("done in %s — trust is not a guarantee of truth; it is the attributability of lies.\n", time.Since(start).Round(time.Second))
}

// runProvin runs one `provin` CLI invocation and returns its combined
// output; ok reflects the exit status — the same shape archiveverify's own
// provin() helper uses, kept local here since a test-file helper cannot be
// imported across packages.
func runProvin(t *testing.T, cli string, args ...string) (output string, ok bool) {
	t.Helper()
	cmd := exec.Command(cli, args...)
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err := cmd.Run()
	return buf.String(), err == nil
}
