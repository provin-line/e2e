package harness

import (
	"fmt"
	"sort"
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
	ResolverBaseURL string // single-registry override; mutually exclusive with RegistryBaseURLs
	// RegistryBaseURLs maps registry ids to base URLs (multi-registry
	// topologies: each org's node hosts its own registry). Rendered as the
	// chain.nats.registry-base-urls block with quoted keys.
	RegistryBaseURLs map[string]string
	VCStoreEndpoint  string // optional; producing loops publish credentials here
	LoopsBlock       string // contents of pipeline.loops { ... }, may be empty

	// SSRF-guard opt-ins. Process runtime nodes talk to loopback peers
	// (AllowLoopback); compose runtime nodes talk over the container network's
	// RFC 1918 addresses (AllowPrivateNetworks).
	AllowLoopback        bool
	AllowPrivateNetworks bool

	// Extra is appended verbatim inside provin.network.pipeline for node-level
	// tuning overrides (e.g. audit-runner intervals). Optional.
	Extra string
	// ChainExtra is appended verbatim inside provin.network.chain, after the
	// nats block — chain-level posture overrides such as the did-cache block.
	// Optional.
	ChainExtra string
}

// Render produces the application.conf text.
// serviceEndpointsBlock advertises the node's VC-resolver service in every
// DID document it issues — the normative endpoint derivation the batch
// resolver and the aggregate-complete bundle exporter both follow. A node
// that publishes credentials (VCStoreEndpoint set) advertises where they
// resolve; a node without one advertises nothing.
func (c NodeConfig) serviceEndpointsBlock() string {
	if c.VCStoreEndpoint == "" {
		return ""
	}
	return fmt.Sprintf("    service-endpoints {\n      vc-resolver { type = \"VCResolver\", url = %q }\n    }\n", c.VCStoreEndpoint)
}

func (c NodeConfig) Render() string {
	var b strings.Builder
	fmt.Fprintf(&b, `provin.network {
  core {
    listen-addr = %q
    data-dir    = "./data"
    dev.allow-loopback     = %v
    allow-private-networks = %v
    # This harness never configures node-native TLS (no cert/key) and the
    # process-runtime listen-addr (FreePort's ":<port>") has no host segment,
    # so core.ListenerIsLoopback cannot recognize it as loopback even though
    # the harness only ever binds 127.0.0.1 — an explicit cleartext
    # acknowledgement is the honest choice here (P0-6 transport-security
    # guard), never a real deployment posture.
    tls.allow-cleartext = true
  }
  auth.policy-verifier-url = %q
  registry {
    id = %q
%s  }
  chain {
    transport = "nats"
    nats {
      url                  = %q
      account-seed-file    = %q
      trust-root-seed-file = %q
      resolver-dir         = %q
      node-did             = %q
      resolver-base-url    = %q
`, c.ListenAddr, c.AllowLoopback, c.AllowPrivateNetworks, c.PDPBaseURL, c.RegistryID,
		c.serviceEndpointsBlock(),
		c.NATSURL, c.AccountSeedFile, c.TrustSeedFile, c.ResolverDir, c.NodeDID, c.ResolverBaseURL)
	if len(c.RegistryBaseURLs) > 0 {
		b.WriteString("      registry-base-urls {\n")
		regs := make([]string, 0, len(c.RegistryBaseURLs))
		for reg := range c.RegistryBaseURLs {
			regs = append(regs, reg)
		}
		sort.Strings(regs)
		for _, reg := range regs {
			fmt.Fprintf(&b, "        %q = %q\n", reg, c.RegistryBaseURLs[reg])
		}
		b.WriteString("      }\n")
	}
	b.WriteString("    }\n")
	if c.ChainExtra != "" {
		fmt.Fprintf(&b, "%s\n", c.ChainExtra)
	}
	b.WriteString("  }\n  pipeline {\n")
	if c.VCStoreEndpoint != "" {
		fmt.Fprintf(&b, "    vc-store-endpoint = %q\n", c.VCStoreEndpoint)
	}
	// Always present: the batch resolver reuses this token for peer predecessor
	// fetches, which a consuming-only node (no endpoint) still performs.
	fmt.Fprintf(&b, "    vc-store-bearer   = %q\n", BearerToken)
	if c.LoopsBlock != "" {
		fmt.Fprintf(&b, "    loops {\n%s\n    }\n", c.LoopsBlock)
	}
	if c.Extra != "" {
		fmt.Fprintf(&b, "%s\n", c.Extra)
	}
	b.WriteString("  }\n}\n")
	return b.String()
}
