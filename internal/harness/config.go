package harness

import (
	"fmt"
	"strings"
)

// NodeConfig renders the standalone node's application.conf for a scenario.
// LoopsBlock is the scenario-authored `loops { ... }` HOCON body (the loop
// topology IS the scenario, so it stays in the scenario file, not the harness).
type NodeConfig struct {
	ListenAddr      string // e.g. ":18443"
	RegistryID      string // did:dplaax {registry} segment
	PDPBaseURL      string // allow-all PDP stub base URL
	NATSURL         string
	AccountSeedFile string
	TrustSeedFile   string
	ResolverDir     string
	NodeDID         string
	ResolverBaseURL string // usually the node's own base URL (single-registry override)
	VCStoreEndpoint string // optional; producing loops publish credentials here
	LoopsBlock      string // contents of pipeline.loops { ... }, may be empty

	// Extra is appended verbatim inside provin.network.pipeline for node-level
	// tuning overrides (e.g. audit-runner intervals). Optional.
	Extra string
}

// Render produces the application.conf text.
func (c NodeConfig) Render() string {
	var b strings.Builder
	fmt.Fprintf(&b, `provin.network {
  core {
    listen-addr = %q
    data-dir    = "./data"
    # e2e nodes talk to loopback peers (PDP stub, their own resolver route).
    dev.allow-loopback = true
  }
  auth.policy-verifier-url = %q
  registry { id = %q }
  chain {
    transport = "nats"
    nats {
      url                  = %q
      account-seed-file    = %q
      trust-root-seed-file = %q
      resolver-dir         = %q
      node-did             = %q
      resolver-base-url    = %q
    }
  }
  pipeline {
`, c.ListenAddr, c.PDPBaseURL, c.RegistryID,
		c.NATSURL, c.AccountSeedFile, c.TrustSeedFile, c.ResolverDir, c.NodeDID, c.ResolverBaseURL)
	if c.VCStoreEndpoint != "" {
		fmt.Fprintf(&b, "    vc-store-endpoint = %q\n", c.VCStoreEndpoint)
		fmt.Fprintf(&b, "    vc-store-bearer   = %q\n", BearerToken)
	}
	if c.LoopsBlock != "" {
		fmt.Fprintf(&b, "    loops {\n%s\n    }\n", c.LoopsBlock)
	}
	if c.Extra != "" {
		fmt.Fprintf(&b, "%s\n", c.Extra)
	}
	b.WriteString("  }\n}\n")
	return b.String()
}
