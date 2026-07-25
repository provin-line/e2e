package harness

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// openComposeDeferrals reads FINDINGS.md and returns the scenarios currently
// licensed to ship without a compose twin, mapped to the finding that licenses
// them.
//
// A block qualifies only if BOTH hold: it declares `**Status**: open`, and it
// carries a `**Compose twin deferred**` field naming `scenarios/<name>`.
//
// The licence deliberately does NOT come from `**Affected scenario(s)**`, even
// though that field is required on every open finding and would be the obvious
// thing to read. Almost every finding is a product gap that happens to affect
// some scenario; treating those as compose licences would put the register's
// purpose (rule 4: record what the product does not provide) at war with its
// enforcement (rule 3: every scenario ships a twin) — recording an ordinary
// finding against a scenario that already HAS a twin would fail the suite. A
// dedicated field says one thing only, and says it on purpose.
//
// Reading the status loosely would let a resolved finding license a gap
// forever; scanning the whole block for scenario paths instead of just that one
// field would let passing prose ("see scenarios/longchain for the pattern")
// license a missing twin by accident.
func openComposeDeferrals(md string) map[string]string {
	out := map[string]string{}
	// Leading "\n" so a heading at position 0 is a block like any other, and a
	// newline-anchored separator so a mid-line "### " cannot start a finding.
	// A "### " inside a fenced code block IS still treated as a heading — the
	// register has no fenced findings today, and the failure direction is a
	// loud one (a template naming a real scenario trips the guard rather than
	// silencing it).
	for _, block := range strings.Split("\n"+md, "\n### ")[1:] {
		id, _, _ := strings.Cut(block, " ")
		id = strings.TrimSpace(id)
		if !strings.HasPrefix(id, "E2E-F-") {
			continue
		}
		var isOpen bool
		var deferrals []string
		for _, line := range strings.Split(block, "\n") {
			line = strings.TrimSpace(line)
			// A block ends where the next heading of any level begins.
			if strings.HasPrefix(line, "#") {
				break
			}
			if strings.HasPrefix(line, "- **Status**:") {
				isOpen = strings.TrimSpace(strings.TrimPrefix(line, "- **Status**:")) == "open"
			}
			// Only the field's own line is read, so a wrapped continuation is
			// invisible: that fails safe (the guard demands the twin), and
			// FINDINGS.md tells authors to keep the field on one line.
			if strings.HasPrefix(line, "- **Compose twin deferred**:") {
				_, field, _ := strings.Cut(line, ":")
				deferrals = append(deferrals, field)
			}
		}
		if !isOpen {
			continue
		}
		for _, field := range deferrals {
			for _, tok := range strings.FieldsFunc(field, func(r rune) bool {
				return r == '`' || r == ',' || r == ' ' || r == '.' || r == ';'
			}) {
				if name, ok := strings.CutPrefix(tok, "scenarios/"); ok && name != "" {
					out[name] = id
				}
			}
		}
	}
	return out
}

// The register is the enforcement surface for AGENTS.md rule 3, so the parser
// that reads it has to be exact about two things.
//
// First, a RESOLVED finding must not license a missing compose twin. `go test`
// exits 0 on a skip, so a loose parser makes the rule silently stop holding —
// which is how the losswindow twin went missing in the first place.
//
// Second, and less obvious: the licence must come from a field that means
// ONLY "this scenario's compose twin may be absent". `Affected scenario(s)` is
// required on EVERY open finding (FINDINGS.md's schema), the vast majority of
// which are product gaps with nothing to do with compose — so keying on it
// would make the register's primary purpose (rule 4) and its enforcement
// (rule 3) mutually exclusive: recording an ordinary finding against a
// scenario that HAS a twin would fail the suite.
func TestOpenComposeDeferralsParsing(t *testing.T) {
	for _, tc := range []struct {
		name string
		md   string
		want []string // "scenario=ID" pairs, sorted
	}{
		{
			name: "an open deferral licenses its scenario",
			md: `### E2E-F-028 — losswindow has no compose twin

- **Status**: open
- **Affected scenario**: ` + "`scenarios/losswindow`" + `
- **Compose twin deferred**: ` + "`scenarios/losswindow`" + `
- **Rationale**: whatever
`,
			want: []string{"losswindow=E2E-F-028"},
		},
		{
			name: "a resolved deferral licenses nothing",
			md: `### E2E-F-028 — losswindow has no compose twin

- **Status**: resolved
- **Compose twin deferred**: ` + "`scenarios/losswindow`" + `
`,
			want: nil,
		},
		{
			// The case that keying on Affected scenario got wrong: an ordinary
			// rule-4 product-gap finding, correctly formed per the schema,
			// against a scenario that ships a twin. It must license nothing.
			name: "an ordinary finding naming an affected scenario licenses nothing",
			md: `### E2E-F-029 — the product exposes no surface for X

- **Status**: open
- **Affected scenario**: ` + "`scenarios/recall`" + `
- **Rationale**: the product lacks a surface; the harness works around it
`,
			want: nil,
		},
		{
			name: "one open, one resolved",
			md: `### E2E-F-028 — a

- **Status**: resolved
- **Compose twin deferred**: scenarios/losswindow

### E2E-F-029 — b

- **Status**: open
- **Compose twin deferred**: scenarios/branching
`,
			want: []string{"branching=E2E-F-029"},
		},
		{
			name: "several scenarios deferred by one finding",
			md: `### E2E-F-031 — two scenarios deferred together

- **Status**: open
- **Compose twin deferred**: scenarios/alpha, scenarios/beta
`,
			want: []string{"alpha=E2E-F-031", "beta=E2E-F-031"},
		},
		{
			name: "prose mentioning a scenario path is not a deferral",
			md: `### E2E-F-032 — something else

- **Status**: open
- **Rationale**: see scenarios/longchain for the pattern
`,
			want: nil,
		},
		{
			// A status the schema does not allow reads as not-open. That fails
			// SAFE — the guard demands the twin — but pin it so the direction
			// is a decision rather than an accident.
			name: "a qualified status is not open",
			md: `### E2E-F-033 — blocked on something

- **Status**: open (blocked)
- **Compose twin deferred**: scenarios/alpha
`,
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := openComposeDeferrals(tc.md)
			var pairs []string
			for scenario, id := range got {
				pairs = append(pairs, scenario+"="+id)
			}
			sort.Strings(pairs)
			if strings.Join(pairs, ",") != strings.Join(tc.want, ",") {
				t.Errorf("openComposeDeferrals = %v, want %v", pairs, tc.want)
			}
		})
	}
}

// TestComposeParity is the rule-3 preflight: every scenario ships a compose
// twin, and the only licence for a missing one is an OPEN finding in
// FINDINGS.md naming that scenario. A `t.Skip` is not a licence — the suite
// exits 0 on a skip, so without this test a silently dropped twin reads as a
// green run (it did, for `losswindow`, until E2E-F-028).
//
// It also fails in the other direction: a scenario that HAS a twin must not
// still be carrying an open deferral, or the register drifts into claiming
// debt that was already paid.
func TestComposeParity(t *testing.T) {
	root := repoRoot(t)
	md, err := os.ReadFile(filepath.Join(root, "FINDINGS.md"))
	if err != nil {
		t.Fatalf("read FINDINGS.md: %v — rule 3's enforcement surface must exist", err)
	}
	deferred := openComposeDeferrals(string(md))

	entries, err := os.ReadDir(filepath.Join(root, "scenarios"))
	if err != nil {
		t.Fatalf("read scenarios/: %v", err)
	}
	var scenarios int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		scenarios++
		name := e.Name()
		_, statErr := os.Stat(filepath.Join(root, "scenarios", name, "docker-compose.yml"))
		hasTwin := statErr == nil
		id, isDeferred := deferred[name]

		switch {
		case !hasTwin && !isDeferred:
			t.Errorf("scenario %s has no docker-compose.yml and no OPEN finding in FINDINGS.md naming it — AGENTS.md rule 3: author the twin, or record the deferral as an open finding (a t.Skip is not a record)", name)
		case hasTwin && isDeferred:
			t.Errorf("scenario %s has a docker-compose.yml but %s is still open in FINDINGS.md — close the finding with its resolution evidence", name, id)
		}
	}
	if scenarios == 0 {
		t.Fatal("no scenarios found — this guard would pass vacuously")
	}
}
