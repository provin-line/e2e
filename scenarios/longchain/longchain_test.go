// Scenario longchain: a deep serial relay — source → N chained hops → sink —
// on one node. Exercises what simple cannot:
//
//   - a long chain-preserving credential lineage (N+1 credentials head→origin);
//   - the async batch resolver + audit runner assembling and verifying the
//     FULL chain (deep chainwalk), not just the adjacent link;
//   - wire chain traversal: the test walks previousCredential hashes from the
//     sink-consumed head back to the FirstDrop via ResolveVC only, verifying
//     every credential independently and checking the hop ordering — the
//     revived "vcresolver-chain" behavior.
//
// The depth is parameterized (E2E_LONGCHAIN_HOPS, default 10 — the scenario's
// original fixed topology), and TestLongChain_SteadyStatePublishRate turns the
// same topology into a sustained-rate measurement (E2E_LONGCHAIN_RATE) — the
// depth×rate steady-state sweep gate-scaling-results.md records. Both knobs
// flow through the same rendered config on both runtimes, so the compose twin
// stays equivalent by construction (AGENTS.md rule 3): the docker-compose.yml
// topology (one node, broker, PDP stub) is depth-independent.
package longchain

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	ingressSubject = "ingest.deep"

	srcPipelineDID = orgBase + "deep"
	srcProcessDID  = srcPipelineDID + ":process:s1"
)

// hops returns the relay depth. 10 is the scenario's original fixed topology
// and stays the default for the correctness tests; the steady-state sweep
// overrides it per run (depths 2 / 10 / 32 in gate-scaling-results.md).
//
// It is a lazily initialized FUNCTION, not a package-level var, on purpose:
// an os.Getenv in a package-level initializer runs before the testing
// framework's operation log starts recording, so `go test` would not include
// E2E_LONGCHAIN_HOPS in its result-cache key and a depth-32 run could be
// answered from a cached depth-2 result. Reading it at first use inside the
// test records the dependency. (Measurement runs should still pass -count=1.)
func hops() int {
	hopsOnce.Do(func() { hopsN = envInt("E2E_LONGCHAIN_HOPS", 10) })
	return hopsN
}

var (
	hopsOnce sync.Once
	hopsN    int
)

func envInt(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		panic(fmt.Sprintf("%s=%q: want a positive integer", name, v))
	}
	return n
}

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
	for i := 1; i <= hops(); i++ {
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
	for i := 1; i <= hops(); i++ {
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

// startLongChain boots the scenario topology in the selected runtime. The
// did-cache posture is env-selected (E2E_LONGCHAIN_DIDCACHE=1 enables the
// chain.did-cache block on both binaries) so the same tests measure both
// resolver postures without a second scenario.
func startLongChain(t *testing.T) harness.SingleNodeEnv {
	t.Helper()
	pipelines, processes := chainDIDs()
	chainTunables := ""
	if os.Getenv("E2E_LONGCHAIN_DIDCACHE") == "1" {
		chainTunables = "    did-cache { enabled = true }"
	}
	return harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    pipelines,
		ProcessDIDs:     processes,
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		ChainTunables:   chainTunables,
		IngressSubjects: []string{ingressSubject},
	})
}

// relayDeadline scales the single-record relay wait with depth: the original
// fixed depth-10 budget was 90s, and each additional hop adds sequential
// verify/sign/publish work.
func relayDeadline() time.Duration {
	return 90*time.Second + time.Duration(3*hops())*time.Second
}

func TestLongChain_DeepAuditAndWireTraversal(t *testing.T) {
	ctx := context.Background()
	e := startLongChain(t)

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
	harness.WaitFor(t, fmt.Sprintf("sink NDJSON record after %d hops", hops()), relayDeadline(), func() bool {
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
			if p["hop"] != float64(hops()) {
				t.Fatalf("final payload hop = %v, want %d (hops lost in relay)", p["hop"], hops())
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
		if depth > hops()+1 {
			t.Fatalf("chain longer than expected: depth %d > %d", depth, hops()+1)
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
	if len(issuers) != hops()+1 {
		t.Fatalf("walked %d credentials, want %d", len(issuers), hops()+1)
	}
	// Head is the last hop; origin is the source process.
	if issuers[0] != hopProcessDID(hops()) {
		t.Errorf("head issuer = %s, want %s", issuers[0], hopProcessDID(hops()))
	}
	if got, want := issuers[len(issuers)-1], orgBase+"deep:process:s1"; got != want {
		t.Errorf("origin issuer = %s, want %s", got, want)
	}

	// Deep async audit: the full (hops+1)-credential chain records VERIFIED.
	auditClient := auditpbconnect.NewAuditServiceClient(http.DefaultClient, e.NodeBase)
	harness.WaitFor(t, "deep-chain audit VERIFIED", relayDeadline(), func() bool {
		st, err := auditClient.GetAuditStatus(ctx, harness.Bearer(connect.NewRequest(&auditpb.GetAuditStatusRequest{HeadHash: head})))
		if err != nil {
			return false
		}
		lc := st.Msg.GetLinearChain()
		return lc != nil && lc.GetConfidence() == auditpb.Confidence_CONFIDENCE_VERIFIED
	})
}

// verifiedSinkCount counts fully relayed, verified deliveries in the sink's
// NDJSON stream — the black-box observable (rule 1) the steady-state rate is
// computed from. A sink record that is NOT verified fails the measurement
// loudly: a rate computed over silently rejected deliveries would be the exact
// silent-reject measurement error the bench repo's rule 5 exists to prevent.
func verifiedSinkCount(t *testing.T, lines []string) int {
	t.Helper()
	n := 0
	for _, line := range lines {
		var rec struct {
			Credential string          `json:"credential"`
			Confidence string          `json:"confidence"`
			Payload    json.RawMessage `json:"payload"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Credential == "" {
			continue
		}
		if !strings.EqualFold(rec.Confidence, "verified") {
			t.Fatalf("sink record not verified during steady-state run: %s", line)
		}
		var p map[string]any
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			t.Fatalf("sink payload: %v", err)
		}
		if p["hop"] != float64(hops()) {
			t.Fatalf("sink record hop = %v, want %d (a partial relay reached the sink)", p["hop"], hops())
		}
		n++
	}
	return n
}

// TestLongChain_SteadyStatePublishRate publishes at a constant offered rate
// and measures the sink's sustained accepted deliveries/s — the steady-state
// records/s figure for a networked single-node deployment at this depth.
// Opt-in: it runs only when E2E_LONGCHAIN_RATE (records/s) is set, because a
// meaningful measurement holds the node under load for E2E_LONGCHAIN_SECONDS
// (default 60) — not a correctness budget the default suite should pay.
//
// Sweep knobs: E2E_LONGCHAIN_HOPS (depth), E2E_LONGCHAIN_RATE (offered
// records/s), E2E_LONGCHAIN_SECONDS (measurement window), E2E_LONGCHAIN_DIDCACHE
// (resolver posture), E2E_RUNTIME (process/compose). The first third of the
// window is warmup (replication, cache fill, batch-resolver settling); the
// rate is computed over the remainder, from sink NDJSON growth alone.
func TestLongChain_SteadyStatePublishRate(t *testing.T) {
	offered := envInt("E2E_LONGCHAIN_RATE", 0)
	if offered <= 0 {
		t.Skip("set E2E_LONGCHAIN_RATE=<records/s> to run the steady-state measurement (see the function comment for the sweep knobs)")
	}
	seconds := envInt("E2E_LONGCHAIN_SECONDS", 60)

	e := startLongChain(t)
	conn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()
	publisher := conn.Publisher(ingressSubject)

	// Constant-rate open-loop stimulus: the publisher never waits for the
	// relay, so the sink's growth under a sustained offered rate IS the
	// steady-state acceptance measurement. The count is atomic and the
	// goroutine is joined before it is read; the ACHIEVED rate is validated
	// below, because a synchronous Publish that starts blocking would
	// silently coalesce ticker ticks and deliver less load than the
	// "offered" label claims.
	stop := make(chan struct{})
	var published atomic.Int64
	var publisherDone sync.WaitGroup
	publishStart := time.Now()
	publisherDone.Add(1)
	go func() {
		defer publisherDone.Done()
		interval := time.Second / time.Duration(offered)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-tick.C:
				if err := publisher.Publish([]byte(fmt.Sprintf(`{"reading":%d}`, i))); err != nil {
					t.Errorf("publish %d: %v", i, err)
					return
				}
				published.Add(1)
			}
		}
	}()

	warmup := time.Duration(seconds/3) * time.Second
	window := time.Duration(seconds)*time.Second - warmup
	time.Sleep(warmup)
	startCount := verifiedSinkCount(t, e.SinkLines())
	startAt := time.Now()
	time.Sleep(window)
	endCount := verifiedSinkCount(t, e.SinkLines())
	elapsed := time.Since(startAt)
	close(stop)
	publisherDone.Wait()
	publishElapsed := time.Since(publishStart)

	achieved := float64(published.Load()) / publishElapsed.Seconds()
	if achieved < 0.9*float64(offered) {
		t.Fatalf("achieved publish rate %.2f/s is below 90%% of the offered %d/s: the broker or the publisher could not sustain the load, so the offered label would be a lie", achieved, offered)
	}
	accepted := float64(endCount-startCount) / elapsed.Seconds()
	if endCount == startCount {
		t.Fatalf("no deliveries accepted during the %v measurement window (offered %d/s at depth %d)", window, offered, hops())
	}
	t.Logf("steady-state: depth=%d offered=%d/s achieved=%.2f/s window=%.0fs accepted=%.2f/s (published=%d, sink=%d)",
		hops(), offered, achieved, elapsed.Seconds(), accepted, published.Load(), endCount)
}
