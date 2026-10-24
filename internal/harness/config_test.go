package harness

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/provin-line/oss/hoconconfig"
)

const plainInternalKey = "provin.network.auth.plain-internal"

// loadRendered reads a rendered node config with provin.oss's own loader, so
// the assertions hold at the path provin.oss reads, not at a text position.
func loadRendered(t *testing.T, rendered string) *hoconconfig.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "application.conf")
	if err := os.WriteFile(path, []byte(rendered), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("E2E_RENDERED_CONFIG", path)
	cfg, err := hoconconfig.LoadFile("E2E_RENDERED_CONFIG")
	if err != nil {
		t.Fatalf("load rendered config: %v\n%s", err, rendered)
	}
	return cfg
}

func assertPlainInternal(t *testing.T, name, rendered string, want []string) {
	t.Helper()
	got, err := loadRendered(t, rendered).StringList(plainInternalKey)
	if err != nil {
		t.Fatalf("%s: %s: %v\n%s", name, plainInternalKey, err, rendered)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: %s = %q, want %q", name, plainInternalKey, got, want)
	}
}

func TestRenderAllowsPlainHTTPToAComposePDP(t *testing.T) {
	assertPlainInternal(t, "node", NodeConfig{PDPBaseURL: "http://pdpstub:9091"}.Render(), []string{"pdpstub:9091"})
}

func TestRenderDefaultsThePort(t *testing.T) {
	assertPlainInternal(t, "node", NodeConfig{PDPBaseURL: "http://pdp"}.Render(), []string{"pdp:80"})
}

func TestSplitNodeConfig_PlainInternalReachesBothBinaries(t *testing.T) {
	networkCfg, pipelineCfg := SplitNodeConfig(SeparatedConfig{
		NetworkListenAddr:  ":8443",
		PipelineListenAddr: ":8444",
		RegistryID:         "reg.example",
		ResolverBaseURL:    "http://127.0.0.1:8443",
		PDPBaseURL:         "http://pdpstub:9091",
	})
	for name, cfg := range map[string]string{"network": networkCfg, "pipeline": pipelineCfg} {
		assertPlainInternal(t, name, cfg, []string{"pdpstub:9091"})
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
