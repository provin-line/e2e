package harness

import (
	"strings"
	"testing"
)

// networkBlock returns the body of the rendered `provin.network { … }` block,
// where provin.oss reads provin.network.auth.*.
func networkBlock(t *testing.T, rendered string) string {
	t.Helper()
	const open = "provin.network {"
	start := strings.Index(rendered, open)
	if start < 0 {
		t.Fatalf("no %q in:\n%s", open, rendered)
	}
	depth := 0
	for i := start + len(open) - 1; i < len(rendered); i++ {
		switch rendered[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return rendered[start+len(open) : i]
			}
		}
	}
	t.Fatalf("unclosed %q in:\n%s", open, rendered)
	return ""
}

func TestRenderAllowsPlainHTTPToAComposePDP(t *testing.T) {
	block := networkBlock(t, NodeConfig{PDPBaseURL: "http://pdpstub:9091"}.Render())
	if !strings.Contains(block, `auth.plain-internal = ["pdpstub:9091"]`) {
		t.Fatalf("provin.network block lacks the plain-internal entry:\n%s", block)
	}
}

func TestRenderDefaultsThePort(t *testing.T) {
	block := networkBlock(t, NodeConfig{PDPBaseURL: "http://pdp"}.Render())
	if !strings.Contains(block, `auth.plain-internal = ["pdp:80"]`) {
		t.Fatalf("provin.network block lacks pdp:80:\n%s", block)
	}
}

func TestRenderLeavesALoopbackPDPAlone(t *testing.T) {
	for _, pdp := range []string{"http://127.0.0.1:41234", "http://localhost:9091", "http://[::1]:9091"} {
		if out := (NodeConfig{PDPBaseURL: pdp}).Render(); strings.Contains(out, "plain-internal") {
			t.Errorf("%s: rendered a plain-internal entry for a loopback PDP:\n%s", pdp, out)
		}
	}
}

func TestRenderLeavesAnHTTPSOrMalformedPDPAlone(t *testing.T) {
	for _, pdp := range []string{"https://pdp:443", "::not a url", ""} {
		if out := (NodeConfig{PDPBaseURL: pdp}).Render(); strings.Contains(out, "plain-internal") {
			t.Errorf("%q: rendered a plain-internal entry:\n%s", pdp, out)
		}
	}
}
