package harness

import (
	"encoding/json"
	"fmt"
	"sort"
)

// EmissionReconciliation is an emission log read against what a consumer
// actually received. It keeps the three questions a caller must be able to
// fail on SEPARATELY, because they fail independently:
//
//   - Sequences — every record's sequenceNo in log order. This is the only
//     view that shows the shape of the sequence space: a fork back to 1, a
//     gap, or a duplicate. None of those are visible in Lost, because a
//     forked or duplicated record is a DELIVERED record and so never enters
//     the loss set. Deriving continuity from Lost is exactly the mistake that
//     let a forked space pass as healthy.
//   - Lost — the sequenceNo of every record whose credential never reached the
//     consumer. This is the loss window, and it is nameable only because the
//     log is durable and signed.
//   - Unlogged — credentials the consumer holds that appear in no record. A
//     non-empty Unlogged means the log under-reports what was emitted, so
//     Lost cannot be trusted either.
//
// Sequences and Lost preserve log order. Unlogged cannot — its elements are by
// definition absent from the log — so it is sorted, which is what makes a
// failure message naming two or more unlogged credentials reproducible.
//
// None of the three is keyed by credential hash: two records may legitimately
// carry the same hash, and a hash-keyed accounting would silently drop one.
type EmissionReconciliation struct {
	Sequences []string
	Lost      []string
	Unlogged  []string
}

// ReconcileEmissionLog parses emission-log record payloads against the set of
// credential hashes the consumer received. A malformed payload is an error,
// never a skipped record — an unreadable log must not read as "nothing was
// lost".
//
// payloads MUST be the log's COMPLETE record range, in log order. ListLogRecords
// is paged, and a short page silently changes what every view means: delivered
// credentials past the page boundary surface as Unlogged, and Sequences
// describes a truncated space. Callers pin the expected record count before
// calling. The index in a parse error is positional within payloads, not the
// record's own log index.
func ReconcileEmissionLog(payloads [][]byte, delivered map[string]bool) (EmissionReconciliation, error) {
	var out EmissionReconciliation
	logged := make(map[string]bool, len(payloads))
	for i, payload := range payloads {
		var em struct {
			CredentialHash string `json:"credentialHash"`
			SequenceNo     string `json:"sequenceNo"`
		}
		if err := json.Unmarshal(payload, &em); err != nil {
			return EmissionReconciliation{}, fmt.Errorf("emission record %d: %w", i, err)
		}
		out.Sequences = append(out.Sequences, em.SequenceNo)
		logged[em.CredentialHash] = true
		if !delivered[em.CredentialHash] {
			out.Lost = append(out.Lost, em.SequenceNo)
		}
	}
	// Go randomizes map iteration, so sort: an unsorted Unlogged would make the
	// caller's failure message vary run to run for the same defect.
	for h := range delivered {
		if !logged[h] {
			out.Unlogged = append(out.Unlogged, h)
		}
	}
	sort.Strings(out.Unlogged)
	return out, nil
}
