package harness

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestStartSeparatedNode_MinimalSourceLoop proves the harness's
// separated-topology support: cmd/network (control plane) and cmd/pipeline
// (data plane) booted as two independent processes talking only over the
// wire (cmd/pipeline's vc-store-endpoint pointed at cmd/network's base URL)
// and the harness's own embedded NATS broker — the production topology's
// process-mode twin BuildStandalone/StartNode gives the (retiring) all-in-one
// binary. Mirrors oss's own cmd/pipeline/bootsmoke_test.go
// (TestPipeline_ActualBoot: a single source loop, zero stimulus) plus
// cmd/network/bootsmoke_test.go, driven through this package's own
// abstractions instead of hand-built HOCON fixtures.
//
// No credential is ever published or resolved here (no stimulus), so this
// smoke test needs no cross-process DID resolution — see ProvisionPipelineKey's
// doc for why that would be a genuine, currently-unsolved black-box gap for
// any scenario that DOES need one.
func TestStartSeparatedNode_MinimalSourceLoop(t *testing.T) {
	networkBin, pipelineBin := BuildBinaries(t)

	workDir := t.TempDir()
	broker := StartNATS(t, filepath.Join(workDir, "nats"), "acme")
	acct := broker.Account(t, "acme")
	pdpURL := StartPDPStub(t, FreePort(t))

	networkListen := FreePort(t)
	pipelineListen := FreePort(t)
	networkBaseURL := "http://127.0.0.1" + networkListen

	const (
		nodeDID   = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:p1:process:node"
		srcOutput = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src"
		srcIssuer = "did:dplaax:poc.dplaax.dev:org:acme:pipeline:src:process:s1"
	)
	loops := fmt.Sprintf(`
      src {
        role            = "source"
        ingress-subject = "ingest.src"
        output-subject  = %q
        issuer {
          did                 = %q
          key-id              = "signing"
          verification-method = %q
        }
        pipeline-id          = "src"
        process-id           = "s1"
        transformation-claim = "convert"
      }`, srcOutput, srcIssuer, srcIssuer+"#signing")

	networkCfg, pipelineCfg := SplitNodeConfig(SeparatedConfig{
		NetworkListenAddr:  networkListen,
		PipelineListenAddr: pipelineListen,
		RegistryID:         "poc.dplaax.dev",
		PDPBaseURL:         pdpURL,
		NATSURL:            broker.URL,
		AccountSeedFile:    acct.SeedFile,
		TrustSeedFile:      broker.TrustSeedFile,
		ResolverDir:        broker.ResolverDir,
		NodeDID:            nodeDID,
		ResolverBaseURL:    networkBaseURL,
		AllowLoopback:      true,
		LoopsBlock:         loops,
	})

	pipelineDir := filepath.Join(workDir, "acme-pipeline")
	pipelineDataDir := filepath.Join(pipelineDir, "data")
	// The D9 payload-retain preflight (guard 5) and the node-identity/
	// custody-log-signer preflight (guard 6) — see ProvisionPipelineKey's doc.
	ProvisionPipelineKey(t, pipelineDataDir, srcOutput)
	ProvisionPipelineKey(t, pipelineDataDir, nodeDID)
	ProvisionPipelineKey(t, pipelineDataDir, srcIssuer)

	sn := StartSeparatedNode(t, SeparatedNodeSpec{
		Name: "acme",

		NetworkBin:        networkBin,
		NetworkListenAddr: networkListen,
		NetworkDir:        filepath.Join(workDir, "acme-network"),
		NetworkConfig:     networkCfg,

		PipelineBin:        pipelineBin,
		PipelineListenAddr: pipelineListen,
		PipelineDir:        pipelineDir,
		PipelineConfig:     pipelineCfg,
	})

	if lines := sn.SinkLines(); len(lines) != 0 {
		t.Errorf("SinkLines before any stimulus = %v, want none", lines)
	}
	if sn.BaseURL != networkBaseURL {
		t.Errorf("BaseURL = %q, want the network process's base URL %q", sn.BaseURL, networkBaseURL)
	}

	sn.Stop(t)
}
