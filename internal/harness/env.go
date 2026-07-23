package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// SingleNodeEnv is what a single-node scenario body needs from either runtime.
type SingleNodeEnv struct {
	NodeBase string // control-plane base URL, host-reachable
	// PipelineBase is the data-plane base URL, host-reachable — where an
	// HTTP-facing data-plane surface lives (cmd/pipeline/push.go's
	// /ingest/<loop>/push and /health routes; a push-enabled loop's HTTP
	// ingress). On the compose runtime (still all-in-one; A2 migrates the
	// process runtime only) it equals NodeBase — one process serves both
	// planes. On the process runtime it is cmd/pipeline's OWN base URL,
	// DIFFERENT from NodeBase (cmd/network's) — the separated topology's data
	// plane and control plane are two processes on two ports.
	PipelineBase string
	NATSURL      string // broker URL, host-reachable
	AcctSeed     string // the account seed for producer connections
	SinkLines    func() []string
	// RestartNode stops and restarts the standalone node with its SAME data
	// dir, blocking until it is healthy and its loops resubscribed (the
	// spec's IngressSubjects), and returns the node's base URL VALID AFTER
	// the restart. A deployment restart: file-backed state survives,
	// in-memory state is lost. Two runtime divergences a scenario must
	// respect: (1) the compose runtime re-publishes ephemeral host ports on
	// restart, so the pre-restart NodeBase may be stale — use the returned
	// URL (and rebuild clients) for everything after the restart; (2) sink
	// output — the process runtime starts a fresh stream, compose
	// accumulates docker logs — so match sink records with payloads distinct
	// per phase.
	RestartNode func() string
	// StopNode stops the node and leaves it down (the broker keeps running).
	StopNode func()
}

// SingleNodeSpec describes a one-node scenario topology: one NATS account, one
// owner, and the Pipeline/Process DIDs its loops sign as. Account doubles as
// the compose service name and the seed-file prefix; combined with
// RegistryID it also derives the owner DID StartSingleNode registers
// ("did:dplaax:{RegistryID}:org:{Account}", the same shape every scenario's
// own ownerDID constant already used before this field existed).
type SingleNodeSpec struct {
	Account    string
	RegistryID string
	// NodeDID is the chain.nats.node-did value — the identity cmd/pipeline's
	// own wireauth-signed calls sign as (RegisterAuditHead, ResolvePayload;
	// wiring.go's preflightWireOnlySignerKeys). In the separated (process)
	// runtime it MUST be one of PipelineDIDs/ProcessDIDs below — the owner DID
	// (this field's pre-A2 convention) has no #auth verification method
	// (RegisterOwner's document carries only #signing) and so can never
	// resolve for wireauth; StartSingleNode's process branch fails fast if
	// this invariant is violated. The compose (all-in-one) runtime has no wire
	// boundary to cross here and tolerates either.
	NodeDID string
	// PipelineDIDs / ProcessDIDs are every Pipeline/Process DID the scenario's
	// loops sign as (issuer.did in Loops' rendered config, and any DID used as
	// a producing loop's output-subject). StartSingleNode issues each one
	// under the derived owner before returning — mint mode (server-side keys)
	// on the compose runtime, the external-key path (ProvisionExternalIdentity
	// + BootstrapExternal) on the process runtime, where the minted key must
	// live in cmd/pipeline's OWN data dir, not the registry's (D9 keystore
	// locality — ProvisionPipelineKey's doc).
	PipelineDIDs []string
	ProcessDIDs  []string
	// Loops renders the pipeline.loops body; selfBase is the node's own
	// control-plane base URL AS THE NODE REACHES IT (loopback in process mode,
	// the compose service name in compose mode).
	Loops func(selfBase string) string
	// Tunables is appended as node-level pipeline config (batch-resolver /
	// audit-runner intervals). Empty = reference.conf defaults.
	Tunables string
	// IngressSubjects are the raw-ingest subjects the scenario publishes to.
	// StartSingleNode blocks until each has a subscriber, so a stimulus cannot
	// be published before the node's loops subscribed (plain-subject publishes
	// with no subscriber are lost).
	IngressSubjects []string
}

// ownerDIDFor derives a SingleNodeSpec's owner DID from RegistryID + Account —
// the "did:dplaax:{registry}:org:{account}" shape every scenario's own
// ownerDID constant already followed before StartSingleNode absorbed
// Bootstrap.
func ownerDIDFor(spec SingleNodeSpec) string {
	return fmt.Sprintf("did:dplaax:%s:org:%s", spec.RegistryID, spec.Account)
}

// FastTunables are the node-level intervals scenarios use so async machinery
// (batch resolver, audit runner) converges in test time.
const FastTunables = `    batch-resolver { interval = 1s, batch-size = 64, max-retries = 5, max-depth = 1024 }
    audit-runner { interval = 1s, batch-size = 64, max-attempts = 10 }`

// StartSingleNode boots the spec's topology in the selected runtime
// (E2E_RUNTIME=compose → containers; default → subprocesses).
func StartSingleNode(t *testing.T, spec SingleNodeSpec) SingleNodeEnv {
	t.Helper()
	if ComposeRuntime() {
		return startSingleNodeCompose(t, spec)
	}
	return startSingleNodeProcess(t, spec)
}

// startSingleNodeProcess is the separated topology's process-mode twin (A2):
// cmd/network (control plane) + cmd/pipeline (data plane) as two real
// subprocesses, replacing the retiring all-in-one cmd/standalone binary this
// function used to start. Bootstrap now happens HERE, not in scenario code —
// the external-key path needs the pipeline's own data dir (to provision
// local keys BEFORE cmd/pipeline boots, D9 keystore locality), which no
// scenario-level Bootstrap call had access to.
func startSingleNodeProcess(t *testing.T, spec SingleNodeSpec) SingleNodeEnv {
	t.Helper()
	networkBin, pipelineBin := BuildBinaries(t)
	networkListen, pipelineListen := FreePort(t), FreePort(t)
	pdpURL := StartPDPStub(t, FreePort(t))

	workDir := t.TempDir()
	broker := StartNATS(t, filepath.Join(workDir, "nats"), spec.Account)
	acct := broker.Account(t, spec.Account)

	networkBaseURL := "http://127.0.0.1" + networkListen
	networkDir := filepath.Join(workDir, spec.Account+"-network")
	pipelineDir := filepath.Join(workDir, spec.Account+"-pipeline")
	pipelineDataDir := filepath.Join(pipelineDir, "data")

	// Local key provisioning before cmd/pipeline ever boots: its own D9/
	// wire-only-signer preflights fail closed at boot if a configured
	// identity's local key is missing (ProvisionExternalIdentity's doc).
	extKeys := make(map[string]ExternalKeys, len(spec.PipelineDIDs)+len(spec.ProcessDIDs))
	for _, d := range spec.PipelineDIDs {
		extKeys[d] = ProvisionExternalIdentity(t, pipelineDataDir, d)
	}
	for _, d := range spec.ProcessDIDs {
		extKeys[d] = ProvisionExternalIdentity(t, pipelineDataDir, d)
	}
	if _, ok := extKeys[spec.NodeDID]; !ok {
		t.Fatalf("SingleNodeSpec %s: NodeDID %s must be one of PipelineDIDs/ProcessDIDs in the process (separated) runtime — it is the identity cmd/pipeline's own wireauth-signed calls (RegisterAuditHead, ResolvePayload) sign as, and only those two lists get provisioned", spec.Account, spec.NodeDID)
	}

	networkCfg, pipelineCfg := SplitNodeConfig(SeparatedConfig{
		NetworkListenAddr:  networkListen,
		PipelineListenAddr: pipelineListen,
		RegistryID:         spec.RegistryID,
		PDPBaseURL:         pdpURL,
		NATSURL:            broker.URL,
		AccountSeedFile:    acct.SeedFile,
		TrustSeedFile:      broker.TrustSeedFile,
		ResolverDir:        broker.ResolverDir,
		NodeDID:            spec.NodeDID,
		ResolverBaseURL:    networkBaseURL,
		AllowLoopback:      true,
		LoopsBlock:         spec.Loops(networkBaseURL),
		Tunables:           spec.Tunables,
	})

	startBoth := func() *SeparatedNode {
		return StartSeparatedNode(t, SeparatedNodeSpec{
			Name: spec.Account,

			NetworkBin:        networkBin,
			NetworkListenAddr: networkListen,
			NetworkDir:        networkDir,
			NetworkConfig:     networkCfg,

			PipelineBin:        pipelineBin,
			PipelineListenAddr: pipelineListen,
			PipelineDir:        pipelineDir,
			PipelineConfig:     pipelineCfg,
		})
	}
	sn := startBoth()
	waitSubscribed := func() {
		for _, subj := range spec.IngressSubjects {
			broker.WaitForSubscriber(t, subj, 30*time.Second)
		}
	}
	waitSubscribed()

	// Wire bootstrap AFTER both processes are healthy: neither preflight
	// needs a resolvable DID document to boot, only the local keys above —
	// so registering them now (rather than before StartSeparatedNode) needs
	// no boot-order change from Bootstrap's own all-in-one convention.
	owner := NewOwner(t, ownerDIDFor(spec))
	BootstrapExternal(t, sn.BaseURL, owner, spec.PipelineDIDs, spec.ProcessDIDs, extKeys)

	return SingleNodeEnv{
		NodeBase:     sn.BaseURL,
		PipelineBase: "http://127.0.0.1" + pipelineListen,
		NATSURL:      broker.URL,
		AcctSeed:     acct.Seed,
		SinkLines:    func() []string { return sn.SinkLines() },
		RestartNode: func() string {
			sn.Stop(t)
			sn = startBoth()
			waitSubscribed()
			return sn.BaseURL // the process runtime rebinds the same ports
		},
		StopNode: func() { sn.Stop(t) },
	}
}

func startSingleNodeCompose(t *testing.T, spec SingleNodeSpec) SingleNodeEnv {
	t.Helper()
	scenarioDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testdata := filepath.Join(scenarioDir, "testdata")
	if err := os.RemoveAll(testdata); err != nil {
		t.Fatal(err)
	}

	prov := ProvisionCompose(t, testdata, spec.Account)
	prov.WriteBrokerConfig(t)

	selfBase := "http://" + spec.Account + ":8443"
	prov.WriteNodeConfig(t, spec.Account, NodeConfig{
		AllowPrivateNetworks: true,
		ListenAddr:           ":8443",
		RegistryID:           spec.RegistryID,
		PDPBaseURL:           "http://pdpstub:9091",
		NATSURL:              "nats://nats:4222",
		AccountSeedFile:      "/app/secrets/" + spec.Account + "-account.seed",
		TrustSeedFile:        "/app/secrets/operator.seed",
		ResolverDir:          "/app/jwts",
		NodeDID:              spec.NodeDID,
		ResolverBaseURL:      selfBase,
		VCStoreEndpoint:      selfBase,
		LoopsBlock:           spec.Loops(selfBase),
		Extra:                spec.Tunables,
	})

	c := ComposeUp(t, scenarioDir)
	natsMon := c.Port(t, "nats", 8222)
	WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)
	nodeAddr := c.Port(t, spec.Account, 8443)
	nodeBase := "http://" + nodeAddr
	WaitHTTPHealthy(t, spec.Account, nodeBase+"/healthz", 60*time.Second)
	for _, subj := range spec.IngressSubjects {
		WaitForSubscriberHTTP(t, "http://"+natsMon, subj, 60*time.Second)
	}

	// Wire bootstrap: mint mode, unchanged — the compose runtime is still the
	// all-in-one topology (A2 migrates the process runtime only; the compose
	// twin's own migration is a recorded follow-up, AGENTS.md rule 3), so the
	// registry-generated key lands in the SAME container/data-dir the node
	// reads from, same as it always has.
	owner := NewOwner(t, ownerDIDFor(spec))
	Bootstrap(t, nodeBase, owner, spec.PipelineDIDs, spec.ProcessDIDs)

	seed, err := os.ReadFile(filepath.Join(testdata, spec.Account+"-account.seed"))
	if err != nil {
		t.Fatal(err)
	}
	return SingleNodeEnv{
		NodeBase:     nodeBase,
		PipelineBase: nodeBase, // still all-in-one: one process serves both planes
		NATSURL:      "nats://" + c.Port(t, "nats", 4222),
		AcctSeed:     strings.TrimSpace(string(seed)),
		SinkLines:    func() []string { return c.SinkLines(t, spec.Account) },
		RestartNode: func() string {
			c.RestartService(t, spec.Account)
			// Ephemeral host ports are re-allocated on container restart:
			// rediscover the published mapping before waiting on it.
			newBase := "http://" + c.Port(t, spec.Account, 8443)
			WaitHTTPHealthy(t, spec.Account, newBase+"/healthz", 60*time.Second)
			for _, subj := range spec.IngressSubjects {
				WaitForSubscriberHTTP(t, "http://"+natsMon, subj, 60*time.Second)
			}
			return newBase
		},
		StopNode: func() { c.StopService(t, spec.Account) },
	}
}
