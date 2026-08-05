package harness

import (
	"strings"
	"testing"
)

// A cached-posture sweep that silently lost the did-cache block on ONE of the
// two binaries would measure a half-cached node, and the black-box scenarios
// have no observable to catch that besides the rate itself. This pins that
// ChainExtra reaches BOTH rendered configs, inside the chain block.
func TestSplitNodeConfig_ChainExtraReachesBothBinaries(t *testing.T) {
	networkCfg, pipelineCfg := SplitNodeConfig(SeparatedConfig{
		NetworkListenAddr:  ":8443",
		PipelineListenAddr: ":8444",
		RegistryID:         "reg.example",
		ResolverBaseURL:    "http://127.0.0.1:8443",
		ChainExtra:         "    did-cache { enabled = true }",
	})
	for name, cfg := range map[string]string{"network": networkCfg, "pipeline": pipelineCfg} {
		if !strings.Contains(cfg, "did-cache { enabled = true }") {
			t.Errorf("%s config lost the ChainExtra block:\n%s", name, cfg)
		}
		// Inside provin.network.chain: the block must appear before the chain
		// block closes and after the nats block does — cheap structural check:
		// the rendered text places it between "    }" (nats close) and the
		// "  pipeline {" that follows the chain block.
		chainIdx := strings.Index(cfg, "chain {")
		pipelineIdx := strings.Index(cfg, "  pipeline {")
		extraIdx := strings.Index(cfg, "did-cache { enabled = true }")
		if !(chainIdx < extraIdx && extraIdx < pipelineIdx) {
			t.Errorf("%s config renders ChainExtra outside the chain block (chain@%d extra@%d pipeline@%d)", name, chainIdx, extraIdx, pipelineIdx)
		}
	}
}
