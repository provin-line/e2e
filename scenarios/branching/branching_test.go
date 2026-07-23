// Scenario branching: one source fans out to two chained branches with
// complementary JSONata filters (reading >= 10 / reading < 10); each branch
// re-signs into its own pipeline subject consumed by its own sink.
//
// Story checks two things the simple scenario cannot:
//   - transport fan-out: two chained loops subscribed to the SAME upstream
//     subject must BOTH receive every event (no accidental queue-group split);
//   - filter provenance: an event dropped by a branch's filter produces no
//     sink record and no credential on that branch (silent StatusFiltered),
//     while the passing branch's chain audits VERIFIED end to end.
package branching

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	natstransport "github.com/provin-line/oss/pipeline/transport/nats"

	"github.com/provin-line/e2e/internal/harness"
)

const (
	registryID = "poc.dplaax.dev"
	ownerDID   = "did:dplaax:poc.dplaax.dev:org:acme"

	srcPipelineDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:readings"
	srcProcessDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:readings:process:s1"
	highPipelineDID = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:high"
	highProcessDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:high:process:h1"
	lowPipelineDID  = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:low"
	lowProcessDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:low:process:l1"

	ingressSubject = "ingest.readings"
)

func chainedBlock(selfBase, name, outPipeline, processDID, filterExpr, branchMark string) string {
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
          transformation-claim  = "filter-convert"
          verification-strategy = "adjacent"
          upstream-endpoint     = %q
          filters               = [%q]
          converter             = "$merge([$, {'branch': '%s'}])"
        }
      }`, name, srcPipelineDID, outPipeline, processDID, processDID+"#signing",
		name, strings.ToLower(name)+"1", selfBase, filterExpr, branchMark)
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
        pipeline-id          = "readings"
        process-id           = "s1"
        transformation-claim = "convert"
      }`, ingressSubject, srcPipelineDID, srcProcessDID, srcProcessDID+"#signing")
	return src +
		chainedBlock(selfBase, "high", highPipelineDID, highProcessDID, "reading >= 10", "high") +
		chainedBlock(selfBase, "low", lowPipelineDID, lowProcessDID, "reading < 10", "low") +
		sinkBlock(selfBase, "archive-high", highPipelineDID) +
		sinkBlock(selfBase, "archive-low", lowPipelineDID)
}

type sinkRecord struct {
	Credential string          `json:"credential"`
	Confidence string          `json:"confidence"`
	Payload    json.RawMessage `json:"payload"`
}

// sinkRecords parses every NDJSON sink record currently on the node's stdout.
func sinkRecords(e harness.SingleNodeEnv) []sinkRecord {
	var out []sinkRecord
	for _, line := range e.SinkLines() {
		var rec sinkRecord
		if json.Unmarshal([]byte(line), &rec) == nil && rec.Credential != "" {
			out = append(out, rec)
		}
	}
	return out
}

// branchOf extracts the converter's branch mark from a sink record payload.
func branchOf(rec sinkRecord) (branch string, reading float64) {
	var p map[string]any
	if json.Unmarshal(rec.Payload, &p) != nil {
		return "", 0
	}
	b, _ := p["branch"].(string)
	r, _ := p["reading"].(float64)
	return b, r
}

func TestBranching_FanOutAndFilterDrop(t *testing.T) {
	e := harness.StartSingleNode(t, harness.SingleNodeSpec{
		Account:         "acme",
		RegistryID:      registryID,
		NodeDID:         srcProcessDID,
		PipelineDIDs:    []string{srcPipelineDID, highPipelineDID, lowPipelineDID},
		ProcessDIDs:     []string{srcProcessDID, highProcessDID, lowProcessDID},
		Loops:           loopsBlock,
		Tunables:        harness.FastTunables,
		IngressSubjects: []string{ingressSubject},
	})

	conn, err := natstransport.Connect(context.Background(), natstransport.Config{URL: e.NATSURL, AccountSeed: e.AcctSeed})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer conn.Close()

	// Two events: 42 must surface only on the high branch, 3 only on the low.
	pub := conn.Publisher(ingressSubject)
	if err := pub.Publish([]byte(`{"reading":42}`)); err != nil {
		t.Fatalf("publish 42: %v", err)
	}
	if err := pub.Publish([]byte(`{"reading":3}`)); err != nil {
		t.Fatalf("publish 3: %v", err)
	}

	// Both branch sinks must emit their one verified record.
	harness.WaitFor(t, "one record on each branch sink", 60*time.Second, func() bool {
		var high, low bool
		for _, rec := range sinkRecords(e) {
			switch b, _ := branchOf(rec); b {
			case "high":
				high = true
			case "low":
				low = true
			}
		}
		return high && low
	})

	// Fan-out settled: give the pipeline a moment to surface any misrouted
	// records, then assert the exact delivery matrix.
	time.Sleep(3 * time.Second)
	recs := sinkRecords(e)
	var highReadings, lowReadings []float64
	for _, rec := range recs {
		if !strings.EqualFold(rec.Confidence, "verified") {
			t.Errorf("sink record not verified: %+v", rec)
		}
		switch b, r := branchOf(rec); b {
		case "high":
			highReadings = append(highReadings, r)
		case "low":
			lowReadings = append(lowReadings, r)
		default:
			t.Errorf("sink record without branch mark: %s", rec.Payload)
		}
	}
	if len(highReadings) != 1 || highReadings[0] != 42 {
		t.Errorf("high branch readings = %v, want exactly [42] — filter drop or fan-out split broken", highReadings)
	}
	if len(lowReadings) != 1 || lowReadings[0] != 3 {
		t.Errorf("low branch readings = %v, want exactly [3] — filter drop or fan-out split broken", lowReadings)
	}
}
