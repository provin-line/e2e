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
	// ingress). It is cmd/pipeline's OWN base URL, DIFFERENT from NodeBase
	// (cmd/network's) on BOTH runtimes as of A3 — the separated topology's
	// data plane and control plane are two processes on two ports (process
	// runtime) or two containers on two published ports (compose runtime).
	PipelineBase string
	NATSURL      string // broker URL, host-reachable
	AcctSeed     string // the account seed for producer connections
	SinkLines    func() []string
	// RestartNode stops and restarts BOTH the network and pipeline
	// processes/containers with their SAME data dirs (network first, then
	// pipeline, mirroring StartSeparatedNode's own boot order — pipeline's
	// wireauth-signed calls need a healthy network peer),
	// blocking until each is healthy and the pipeline's loops resubscribed
	// (the spec's IngressSubjects), and returns the NETWORK base URL VALID
	// AFTER the restart. A deployment restart: file-backed state survives,
	// in-memory state is lost. Runtime divergences a scenario must respect:
	// (1) the compose runtime re-publishes ephemeral host ports on restart,
	// so the pre-restart NodeBase (AND PipelineBase — not returned here,
	// since no current scenario needs it post-restart) may be stale — use
	// the returned URL (and rebuild clients) for everything after the
	// restart; (2) sink output — the process runtime starts a fresh stream,
	// compose accumulates docker logs — so match sink records with payloads
	// distinct per phase.
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
	// wiring.go's preflightWireOnlySignerKeys). It MUST be one of
	// PipelineDIDs/ProcessDIDs below on BOTH runtimes (A3: the compose
	// runtime crossed the same control/data-plane wire boundary the process
	// runtime did in A2, once cmd/network + cmd/pipeline became separate
	// compose services) — the owner DID (this field's pre-A2 convention) has
	// no #auth verification method (RegisterOwner's document carries only
	// #signing) and so can never resolve for wireauth; StartSingleNode fails
	// fast on either runtime if this invariant is violated.
	NodeDID string
	// PipelineDIDs / ProcessDIDs are every Pipeline/Process DID the scenario's
	// loops sign as (issuer.did in Loops' rendered config, and any DID used as
	// a producing loop's output-subject). StartSingleNode issues each one
	// under the derived owner before returning, over the external-key path
	// (ProvisionExternalIdentity + BootstrapExternal) on BOTH runtimes as of
	// A3 — the minted key must live in cmd/pipeline's OWN data dir, not the
	// registry's (D9 keystore locality — ProvisionPipelineKey's doc), which
	// is as true of a compose service's own container as it is of a process
	// runtime subprocess's own working directory.
	PipelineDIDs []string
	ProcessDIDs  []string
	// Loops renders the pipeline.loops body; selfBase is the node's own
	// control-plane (cmd/network) base URL AS THE NODE REACHES IT (loopback
	// in process mode, the network service's own DNS name in compose mode —
	// e.g. http://acme-network:8443, never the pipeline service's).
	Loops func(selfBase string) string
	// Tunables is appended as node-level pipeline config (batch-resolver /
	// audit-runner intervals). Empty = reference.conf defaults.
	Tunables string
	// ChainTunables is appended inside provin.network.chain on both binaries
	// (e.g. the did-cache block). Empty = reference.conf defaults.
	ChainTunables string
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
		ChainExtra:         spec.ChainTunables,
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

// startSingleNodeCompose is the separated topology's compose-mode twin of
// startSingleNodeProcess (A3): cmd/network and cmd/pipeline run as TWO
// compose services (<account>-network, <account>-pipeline) rather than one
// all-in-one container, mirroring EXACTLY the process runtime's own split
// (SplitNodeConfig; AGENTS.md rule 3 — both runtimes describe the same
// node/config layout). The pipeline service needs its own local keystore
// BEFORE it boots (D9 keystore locality — ProvisionPipelineKey's doc), so
// bootstrap moves onto the external-key path here too, same as the process
// runtime; mint mode is no longer used by either runtime as of A3.
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

	networkService := spec.Account + "-network"
	pipelineService := spec.Account + "-pipeline"
	networkSelfBase := "http://" + networkService + ":8443"

	prov := ProvisionCompose(t, testdata, spec.Account)
	prov.WriteBrokerConfig(t)

	// Local key provisioning before cmd/pipeline's container ever boots — the
	// container-runtime twin of startSingleNodeProcess's identical loop. The
	// keys land under testdata/<pipelineService>/data so a compose volume
	// mount can put them at the pipeline container's own DataDir/keys
	// (application.conf's data-dir = "./data", relative to the Dockerfile's
	// WORKDIR /app).
	pipelineDataDir := filepath.Join(testdata, pipelineService, "data")
	extKeys := make(map[string]ExternalKeys, len(spec.PipelineDIDs)+len(spec.ProcessDIDs))
	for _, d := range spec.PipelineDIDs {
		extKeys[d] = ProvisionExternalIdentity(t, pipelineDataDir, d)
	}
	for _, d := range spec.ProcessDIDs {
		extKeys[d] = ProvisionExternalIdentity(t, pipelineDataDir, d)
	}
	if _, ok := extKeys[spec.NodeDID]; !ok {
		t.Fatalf("SingleNodeSpec %s: NodeDID %s must be one of PipelineDIDs/ProcessDIDs — it is the identity cmd/pipeline's own wireauth-signed calls (RegisterAuditHead, ResolvePayload) sign as, and only those two lists get provisioned", spec.Account, spec.NodeDID)
	}
	// filestore (ProvisionExternalIdentity's backend) writes 0700 dirs / 0600
	// files — correct when the host test process both mints and reads a key,
	// wrong once that same tree is bind-mounted into a container running as
	// a DIFFERENT uid (see MakeContainerReadable's doc).
	MakeContainerReadable(t, filepath.Join(pipelineDataDir, "keys"))

	networkCfg, pipelineCfg := SplitNodeConfig(SeparatedConfig{
		NetworkListenAddr:    ":8443",
		PipelineListenAddr:   ":8443", // separate containers — no port collision
		RegistryID:           spec.RegistryID,
		PDPBaseURL:           "http://pdpstub:9091",
		NATSURL:              "nats://nats:4222",
		AccountSeedFile:      "/app/secrets/" + spec.Account + "-account.seed",
		TrustSeedFile:        "/app/secrets/operator.seed",
		ResolverDir:          "/app/jwts",
		NodeDID:              spec.NodeDID,
		ResolverBaseURL:      networkSelfBase,
		AllowPrivateNetworks: true,
		LoopsBlock:           spec.Loops(networkSelfBase),
		Tunables:             spec.Tunables,
		ChainExtra:           spec.ChainTunables,
	})
	prov.WriteNodeConfig(t, networkService, networkCfg)
	prov.WriteNodeConfig(t, pipelineService, pipelineCfg)

	c := ComposeUp(t, scenarioDir)
	natsMon := c.Port(t, "nats", 8222)
	WaitHTTPHealthy(t, "nats", "http://"+natsMon+"/healthz", 60*time.Second)

	// /readyz, not /healthz — dependency-aware readiness on both services,
	// matching StartSeparatedNode's own choice (waitHealthy's doc): a
	// scenario stimulating either service is guaranteed its dependencies are
	// actually live, not merely that the HTTP listener accepted a connection.
	networkHostBase := "http://" + c.Port(t, networkService, 8443)
	WaitHTTPHealthy(t, networkService, networkHostBase+"/readyz", 60*time.Second)
	pipelineHostBase := "http://" + c.Port(t, pipelineService, 8443)
	WaitHTTPHealthy(t, pipelineService, pipelineHostBase+"/readyz", 60*time.Second)

	for _, subj := range spec.IngressSubjects {
		WaitForSubscriberHTTP(t, "http://"+natsMon, subj, 60*time.Second)
	}

	// No epoch-settle wait here. BootstrapExternal below uses the DID
	// registration path (bearer + document proof), never wireauth, so it was
	// never subject to the restart-epoch barrier. What the old wait protected
	// was the pipeline's FIRST wireauth-signed emissions once stimulus flows —
	// and those are now retried (re-signed) by the production client until
	// network's boot window clears (PR #23).

	// Wire bootstrap over the external-key path (BootstrapExternal), same as
	// the process runtime: the registry never mints a private key for a
	// separated pipeline's own identities (D9 keystore locality).
	owner := NewOwner(t, ownerDIDFor(spec))
	BootstrapExternal(t, networkHostBase, owner, spec.PipelineDIDs, spec.ProcessDIDs, extKeys)

	seed, err := os.ReadFile(filepath.Join(testdata, spec.Account+"-account.seed"))
	if err != nil {
		t.Fatal(err)
	}

	restartService := func(service string) string {
		c.RestartService(t, service)
		// Ephemeral host ports are re-allocated on container restart:
		// rediscover the published mapping before waiting on it.
		newBase := "http://" + c.Port(t, service, 8443)
		WaitHTTPHealthy(t, service, newBase+"/readyz", 60*time.Second)
		return newBase
	}

	return SingleNodeEnv{
		NodeBase:     networkHostBase,
		PipelineBase: pipelineHostBase,
		NATSURL:      "nats://" + c.Port(t, "nats", 4222),
		AcctSeed:     strings.TrimSpace(string(seed)),
		SinkLines:    func() []string { return c.SinkLines(t, pipelineService) },
		RestartNode: func() string {
			// Network first (StartSeparatedNode's own boot order — pipeline's
			// wireauth-signed calls need a live network peer), then pipeline.
			// No epoch-settle wait: the production client's re-signing retry
			// clears network's fresh restart-epoch boot window on its own
			// (PR #23), same as this function's own initial boot above.
			newNetworkBase := restartService(networkService)
			restartService(pipelineService)
			for _, subj := range spec.IngressSubjects {
				WaitForSubscriberHTTP(t, "http://"+natsMon, subj, 60*time.Second)
			}
			return newNetworkBase
		},
		// Pipeline first — cmd/pipeline's own D8 ordered shutdown drains its
		// mirror shippers into the registry before it exits — then network,
		// mirroring SeparatedNode.Stop's own order.
		StopNode: func() {
			c.StopService(t, pipelineService)
			c.StopService(t, networkService)
		},
	}
}
