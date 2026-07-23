// Separated-topology support: cmd/network (control plane, LTL) + cmd/pipeline
// (data plane, STL) run as two independent processes talking only over the
// wire — the production deployment shape cmd/standalone (all-in-one) is being
// retired in favor of. This file gives scenarios the same black-box surface
// StartNode/NodeConfig give the all-in-one topology: a config-split helper, a
// two-process starter that waits each side healthy on ITS OWN /readyz, and an
// out-of-band local-key provisioning helper for cmd/pipeline's own boot
// preflights (see ProvisionPipelineKey's doc for why this is NOT a full
// identity-provisioning story yet).
package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/provin-line/oss/crypto"
	"github.com/provin-line/oss/crypto/ed25519"
	"github.com/provin-line/oss/keystore"
	"github.com/provin-line/oss/keystore/filestore"
)

// SeparatedConfig is SplitNodeConfig's input: the same fields a scenario
// already provides an all-in-one NodeConfig, plus the ONE additional thing
// the separated topology needs that NodeConfig has no room for — a SECOND
// listen address, since network and pipeline are now two processes instead
// of one. VCStoreEndpoint has no field here: cmd/pipeline carries no
// in-process registry (wiring.go's package doc), so it is always derived as
// the network process's own base URL, never scenario-supplied.
type SeparatedConfig struct {
	NetworkListenAddr  string // e.g. ":18443" (network's own control-plane port)
	PipelineListenAddr string // e.g. ":18444" (pipeline's own port)

	RegistryID string
	PDPBaseURL string

	NATSURL          string
	AccountSeedFile  string
	TrustSeedFile    string
	ResolverDir      string
	NodeDID          string
	ResolverBaseURL  string
	RegistryBaseURLs map[string]string

	// LoopsBlock is the pipeline.loops body (see NodeConfig.LoopsBlock) — the
	// data-plane topology, which now lives ONLY in the pipeline config
	// (cmd/network refuses to boot with any loop configured).
	LoopsBlock string
	// Tunables is appended verbatim inside the NETWORK's provin.network.pipeline
	// block (see NodeConfig.Extra) — batch-resolver/audit-runner tuning. Only
	// cmd/network ever runs those two background runners (main.go), so this
	// belongs on the network side, not the pipeline side.
	Tunables string

	AllowLoopback        bool
	AllowPrivateNetworks bool
}

// SplitNodeConfig renders SeparatedConfig into the TWO configs a separated
// deployment needs, from the same inputs a scenario already gives an
// all-in-one NodeConfig:
//
//   - networkCfg: cmd/network's application.conf — the full NodeConfig shape
//     MINUS pipeline.loops (cmd/network's main.go fatals if any loop is
//     configured — it carries no data plane, TestProdDeps_NoPipelineInNetworkBinary
//     pins this on the production import graph).
//   - pipelineCfg: cmd/pipeline's CONFIG_FILE — the full NodeConfig shape
//     WITH pipeline.loops, its own listen address, and vc-store-endpoint
//     pointed at the NETWORK process's base URL (never itself — cmd/pipeline
//     is a wire CLIENT to the registry, never a server for it).
//
// Both configs share one NodeConfig base (registry/auth/chain fields), so an
// unused block in either (network reads no provin.network.pipeline.loops-free
// pipeline config beyond its own guard; pipeline reads no
// provin.network.registry block at all — it never imports network/pkg/registry)
// is simply ignored, not an error — HOCON does not validate against an
// allowed-keys schema, only required keys are checked. Each process's OWN
// working directory (StartSeparatedNode's NetworkDir/PipelineDir) gives each
// its own "./data" on disk despite the identical relative path both configs
// render.
func SplitNodeConfig(c SeparatedConfig) (networkCfg, pipelineCfg string) {
	networkBaseURL := "http://127.0.0.1" + c.NetworkListenAddr

	base := NodeConfig{
		AllowLoopback:        c.AllowLoopback,
		AllowPrivateNetworks: c.AllowPrivateNetworks,
		RegistryID:           c.RegistryID,
		PDPBaseURL:           c.PDPBaseURL,
		NATSURL:              c.NATSURL,
		AccountSeedFile:      c.AccountSeedFile,
		TrustSeedFile:        c.TrustSeedFile,
		ResolverDir:          c.ResolverDir,
		NodeDID:              c.NodeDID,
		ResolverBaseURL:      c.ResolverBaseURL,
		RegistryBaseURLs:     c.RegistryBaseURLs,
	}

	network := base
	network.ListenAddr = c.NetworkListenAddr
	network.VCStoreEndpoint = networkBaseURL // unused by cmd/network today (its batch resolver/audit runner never read it) but harmless, and keeps this "the full shape minus loops"
	network.LoopsBlock = ""
	network.Extra = c.Tunables

	pipeline := base
	pipeline.ListenAddr = c.PipelineListenAddr
	pipeline.VCStoreEndpoint = networkBaseURL
	pipeline.LoopsBlock = c.LoopsBlock

	return network.Render(), pipeline.Render()
}

// SeparatedNodeSpec is StartSeparatedNode's input: the two rendered configs
// (typically from SplitNodeConfig) plus where each process listens and where
// its own working directory lives.
type SeparatedNodeSpec struct {
	Name string

	NetworkBin        string
	NetworkListenAddr string
	NetworkDir        string // working directory (config/ + data/ live here)
	NetworkConfig     string // rendered application.conf body

	PipelineBin        string
	PipelineListenAddr string
	PipelineDir        string // working directory (pipeline.conf + data/ live here)
	PipelineConfig     string // rendered CONFIG_FILE body
}

// SeparatedNode is one running network+pipeline process pair — the
// process-runtime twin of the production separated topology. Its accessor
// surface mirrors Node's (Stop, Stdout, Stderr, SinkLines) so a scenario
// migrating off StartNode/BuildStandalone keeps its downstream assertions
// unchanged; only the topology setup differs. Network and Pipeline are
// exported directly for anything that needs ONE specific process (e.g. its
// own Stdout/Stderr on a boot failure, or a ConnectRPC client dialing the
// control plane specifically).
type SeparatedNode struct {
	Name string

	Network  *Node // control plane (cmd/network): DID/VC/Audit/Schema/Payload services
	Pipeline *Node // data plane (cmd/pipeline): the configured transport loops

	// BaseURL is the control-plane base URL — where a scenario's ConnectRPC
	// clients (DID resolution, VC/Audit read, Bootstrap, …) dial, mirroring
	// Node.BaseURL's role for an all-in-one node.
	BaseURL string
}

// writeApplicationConf writes cfg to <dir>/config/application.conf — the
// same convention StartNode uses. cmd/network reads it via
// hoconconfig.Load(".", "CONFIG_OVERLAY"), whose layer 2 always looks there
// regardless of whether CONFIG_OVERLAY is set.
func writeApplicationConf(dir, cfg string) error {
	if err := os.MkdirAll(filepath.Join(dir, "config"), 0o755); err != nil {
		return fmt.Errorf("mkdir config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config", "application.conf"), []byte(cfg), 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// writeConfigFile writes cfg to <dir>/pipeline.conf and returns its path —
// cmd/pipeline's CONFIG_FILE convention names one file directly, with no
// fixed default location (hoconconfig.LoadFile("CONFIG_FILE")).
func writeConfigFile(dir, cfg string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}
	path := filepath.Join(dir, "pipeline.conf")
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		return "", fmt.Errorf("write config: %w", err)
	}
	return path, nil
}

// StartSeparatedNode boots the production separated topology as two real
// processes: cmd/network first (its config via the same application.conf
// convention StartNode uses), waited healthy on ITS OWN /readyz, then
// cmd/pipeline (CONFIG_FILE env pointing at a written config file), waited
// healthy on ITS OWN /readyz. Unlike StartNode's /healthz (pure liveness),
// /readyz on both binaries is dependency-aware — network's checks the
// evidence store (+ PDP when external), pipeline's checks NATS + registry
// reachability (+ PDP when a push-ingress loop is configured) — so a scenario
// stimulating either process is guaranteed its dependencies are actually
// live, not merely that the HTTP listener accepted a connection.
func StartSeparatedNode(t *testing.T, spec SeparatedNodeSpec) *SeparatedNode {
	t.Helper()

	networkBaseURL := "http://127.0.0.1" + spec.NetworkListenAddr
	if err := writeApplicationConf(spec.NetworkDir, spec.NetworkConfig); err != nil {
		t.Fatalf("separated node %s: network config: %v", spec.Name, err)
	}
	network := runNodeProcess(t, spec.Name+"-network", spec.NetworkBin, spec.NetworkDir, networkBaseURL, nil)
	waitHealthy(t, network.Name, network.BaseURL+"/readyz", network)

	pipelineBaseURL := "http://127.0.0.1" + spec.PipelineListenAddr
	confPath, err := writeConfigFile(spec.PipelineDir, spec.PipelineConfig)
	if err != nil {
		t.Fatalf("separated node %s: pipeline config: %v", spec.Name, err)
	}
	pipeline := runNodeProcess(t, spec.Name+"-pipeline", spec.PipelineBin, spec.PipelineDir, pipelineBaseURL, []string{"CONFIG_FILE=" + confPath})
	waitHealthy(t, pipeline.Name, pipeline.BaseURL+"/readyz", pipeline)

	return &SeparatedNode{
		Name:     spec.Name,
		Network:  network,
		Pipeline: pipeline,
		BaseURL:  network.BaseURL,
	}
}

// Stop tears down both processes: the pipeline first — cmd/pipeline's own D8
// ordered shutdown drains its mirror shippers into the registry before it
// exits — then the network. Idempotent (Node.Stop is).
func (s *SeparatedNode) Stop(t *testing.T) {
	t.Helper()
	s.Pipeline.Stop(t)
	s.Network.Stop(t)
}

// Stdout returns the PIPELINE process's stdout.
func (s *SeparatedNode) Stdout() string { return s.Pipeline.Stdout() }

// Stderr returns the PIPELINE process's stderr.
func (s *SeparatedNode) Stderr() string { return s.Pipeline.Stderr() }

// SinkLines returns NDJSON sink lines from the PIPELINE process's stdout —
// sink loops run in cmd/pipeline now, not the control-plane process, so that
// is where sink output lands in the separated topology.
func (s *SeparatedNode) SinkLines() []string { return s.Pipeline.SinkLines() }

// ProvisionPipelineKey generates an Ed25519 keypair and saves it as a #auth
// key for subjectDID directly into dataDir/keys — the SAME local keystore
// cmd/pipeline's own boot preflights read via filestore.New(dataDir+"/keys")
// (wiring.go's preflightPayloadRetainKeys — D9, every producing loop's own
// output subject — and preflightWireOnlySignerKeys — the node identity and
// every durable custody log's checkpoint-signer identity, i.e. a producing/
// receipt-issuing loop's own issuer DID). It mirrors cmd/pipeline's own
// bootreject_test.go/bootsmoke_test.go provisionPayloadRetainKey helper
// exactly — the harness's out-of-band equivalent of the operator/CLI
// provisioning step wiring.go's own doc names as "out of this task's scope,
// same convention cmd/standalone follows".
//
// D9 keystore locality: in the all-in-one topology, harness.Bootstrap's
// IssuePipeline/IssueProcess RPCs mint the loop's signing key SERVER-SIDE,
// but that "server" is the SAME process/data-dir the data plane reads from,
// so the minted key is immediately usable. In the separated topology that
// locality breaks: IssuePipeline/IssueProcess mint into the REGISTRY's
// (cmd/network's) own keystore/data-dir — a DIFFERENT process, DIFFERENT
// directory — which cmd/pipeline's local filestore.Sign never reads (no
// production export/import/bind mechanism moves a minted key between the
// two; confirmed by inspection of network/pkg/services/didregistry and
// keystore/filestore — SaveKeyPair/Sign/DeleteKeys is the entire KeyStore
// contract). This function is the harness's replacement PROVISIONING step for
// that case — but it is boot-preflight-only: it does NOT publish a
// resolvable DID document for subjectDID anywhere. oss's own
// cmd/pipeline/separated_e2e_test.go solves that by seeding the registry's
// internal DID store directly (didyaml.Store) and standing up a fake DID-HTTP
// server, both legitimate ONLY because that test lives inside oss's own
// white-box suite; provin.e2e's AGENTS.md black-box rule (no in-process/
// internal-store construction) forbids the harness from doing the same. A
// scenario that needs the registry to VERIFY a signature from a separated
// pipeline's identity (any real credential exchange, as opposed to a
// boot-only smoke test with zero stimulus) has no current black-box path to
// provision that — a product gap to record as a finding when a scenario
// first needs it (A2), not a harness design choice made here.
func ProvisionPipelineKey(t *testing.T, dataDir, subjectDID string) (pub []byte) {
	t.Helper()
	kp, err := (ed25519.Generator{}).Generate()
	if err != nil {
		t.Fatalf("provision pipeline key %s: keygen: %v", subjectDID, err)
	}
	ks := filestore.New(filepath.Join(dataDir, "keys"))
	if err := ks.SaveKeyPair(subjectDID, map[keystore.KeyID]*crypto.KeyPair{keystore.KeyIDAuth: kp}); err != nil {
		t.Fatalf("provision pipeline key %s: save: %v", subjectDID, err)
	}
	return kp.PublicKey
}

// ExternalKeys is one subject DID's local key material for the external-key
// issuance path (BootstrapExternal): the public halves this scenario submits
// to the registry over IssuePipelineRequest/IssueProcessRequest's
// external_public_keys (field 3) — raw 32-byte Ed25519, matching
// didregistry.ExternalPublicKeys/didpb.ExternalPublicKeys exactly. The
// private halves stay only in the pipeline's own local keystore
// (ProvisionExternalIdentity writes them there); the registry never sees
// them.
type ExternalKeys struct {
	AuthPublicKey    []byte
	SigningPublicKey []byte
}

// ProvisionExternalIdentity mints subjectDID's #auth AND #signing Ed25519
// keypairs and saves BOTH into dataDir/keys — the same local keystore
// ProvisionPipelineKey writes into, and the one cmd/pipeline's own boot
// preflights and runtime signing read via filestore.New(dataDir+"/keys").
// Unlike ProvisionPipelineKey (boot-preflight-only, #auth alone), this is
// the full local half of the external-key issuance path: a subject that
// ISSUES credentials (a loop's issuer DID, whose config names key-id
// "signing" — pipeline/runtime/dataplane.go's vcdid.Signer, wired straight
// to this same local keystore via cmd/pipeline's buildDeps) needs #signing
// locally too, not only #auth (which alone covers a subject's wireauth
// role — RegisterAuditHead/RetainPayload/ResolvePayload/MirrorLogSegment —
// the only role ProvisionPipelineKey's callers needed).
//
// It mints BOTH keys for every subject unconditionally, mirroring the
// registry's OWN mint-mode symmetry (didregistry.issue generates and
// SaveKeyPairs both keystore.KeyIDAuth and keystore.KeyIDSigning for a
// target DID regardless of which role actually uses which key) — cheaper
// and less error-prone than this harness trying to track, per subject,
// which of the two roles (wireauth signer vs. credential issuer) it will
// actually play.
//
// Returns the public halves as ExternalKeys, for BootstrapExternal to
// submit over the wire — this function itself publishes no DID document; it
// only provisions the LOCAL side of the identity.
func ProvisionExternalIdentity(t *testing.T, dataDir, subjectDID string) ExternalKeys {
	t.Helper()
	authKP, err := (ed25519.Generator{}).Generate()
	if err != nil {
		t.Fatalf("provision external identity %s: auth keygen: %v", subjectDID, err)
	}
	signKP, err := (ed25519.Generator{}).Generate()
	if err != nil {
		t.Fatalf("provision external identity %s: signing keygen: %v", subjectDID, err)
	}
	ks := filestore.New(filepath.Join(dataDir, "keys"))
	if err := ks.SaveKeyPair(subjectDID, map[keystore.KeyID]*crypto.KeyPair{
		keystore.KeyIDAuth:    authKP,
		keystore.KeyIDSigning: signKP,
	}); err != nil {
		t.Fatalf("provision external identity %s: save: %v", subjectDID, err)
	}
	return ExternalKeys{AuthPublicKey: authKP.PublicKey, SigningPublicKey: signKP.PublicKey}
}
