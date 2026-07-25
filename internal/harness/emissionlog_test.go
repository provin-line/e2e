package harness

import (
	"strings"
	"testing"
)

// rec builds one emission-log record payload.
func rec(hash, seq string) []byte {
	return []byte(`{"credentialHash":"` + hash + `","sequenceNo":"` + seq + `"}`)
}

// The reconciliation exists to answer three separate questions about an
// emission log, and a caller must be able to fail on each independently:
// which sequences never arrived (loss), whether the sequence space is intact
// (continuity), and whether anything the consumer received is missing from
// the log (completeness). Collapsing them — in particular deriving continuity
// from the loss set — is what let a forked sequence space pass unnoticed: a
// forked record is a DELIVERED record, so it never enters the loss set at all.
func TestReconcileEmissionLog(t *testing.T) {
	for _, tc := range []struct {
		name      string
		payloads  [][]byte
		delivered []string
		seqs      []string
		lost      []string
		unlogged  []string
	}{
		{
			name:      "loss window named, sequence space intact",
			payloads:  [][]byte{rec("h1", "1"), rec("h2", "2"), rec("h3", "3"), rec("h4", "4"), rec("h5", "5")},
			delivered: []string{"h1", "h4", "h5"},
			seqs:      []string{"1", "2", "3", "4", "5"},
			lost:      []string{"2", "3"},
		},
		{
			// The regression this whole helper exists for: the post-restart
			// emission forks back to 1 instead of continuing at 5. It is
			// delivered, so the loss set is STILL exactly [2 3] — identical to
			// the healthy case above. Only Sequences distinguishes them.
			name:      "forked sequence space is visible in Sequences, not in Lost",
			payloads:  [][]byte{rec("h1", "1"), rec("h2", "2"), rec("h3", "3"), rec("h4", "4"), rec("h5", "1")},
			delivered: []string{"h1", "h4", "h5"},
			seqs:      []string{"1", "2", "3", "4", "1"},
			lost:      []string{"2", "3"},
		},
		{
			name:      "a gap in the sequence space is visible in Sequences",
			payloads:  [][]byte{rec("h1", "1"), rec("h2", "2"), rec("h3", "4")},
			delivered: []string{"h1", "h2", "h3"},
			seqs:      []string{"1", "2", "4"},
		},
		{
			name:      "delivered credential absent from the log",
			payloads:  [][]byte{rec("h1", "1")},
			delivered: []string{"h1", "hGhost"},
			seqs:      []string{"1"},
			unlogged:  []string{"hGhost"},
		},
		{
			// Two records sharing a credential hash must both be reported:
			// keying the accounting by hash would silently drop one.
			name:      "duplicate credential hash keeps both sequences",
			payloads:  [][]byte{rec("h1", "1"), rec("h1", "2")},
			delivered: []string{"h1"},
			seqs:      []string{"1", "2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := map[string]bool{}
			for _, h := range tc.delivered {
				delivered[h] = true
			}
			got, err := ReconcileEmissionLog(tc.payloads, delivered)
			if err != nil {
				t.Fatalf("ReconcileEmissionLog: %v", err)
			}
			if strings.Join(got.Sequences, ",") != strings.Join(tc.seqs, ",") {
				t.Errorf("Sequences = %v, want %v", got.Sequences, tc.seqs)
			}
			if strings.Join(got.Lost, ",") != strings.Join(tc.lost, ",") {
				t.Errorf("Lost = %v, want %v", got.Lost, tc.lost)
			}
			if strings.Join(got.Unlogged, ",") != strings.Join(tc.unlogged, ",") {
				t.Errorf("Unlogged = %v, want %v", got.Unlogged, tc.unlogged)
			}
		})
	}
}

// A malformed payload is a parse error, never a silently-skipped record: an
// unreadable emission log must not read as "nothing was lost".
func TestReconcileEmissionLogMalformedPayload(t *testing.T) {
	if _, err := ReconcileEmissionLog([][]byte{[]byte("{not json")}, nil); err == nil {
		t.Fatal("ReconcileEmissionLog on a malformed payload: got nil error, want a parse error")
	}
}
