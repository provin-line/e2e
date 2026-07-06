package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// SingleNodeEnv is what a single-node scenario body needs from either runtime.
type SingleNodeEnv struct {
	NodeBase  string // control-plane base URL, host-reachable
	NATSURL   string // broker URL, host-reachable
	AcctSeed  string // the account seed for producer connections
	SinkLines func() []string
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
// standalone node running the given loops. Account doubles as the compose
// service name and the seed-file prefix.
type SingleNodeSpec struct {
	Account    string
	RegistryID string
	NodeDID    string
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

func startSingleNodeProcess(t *testing.T, spec SingleNodeSpec) SingleNodeEnv {
	t.Helper()
	bin := BuildStandalone(t)
	listenAddr := FreePort(t)
	pdpURL := StartPDPStub(t, FreePort(t))

	workDir := t.TempDir()
	broker := StartNATS(t, filepath.Join(workDir, "nats"), spec.Account)
	acct := broker.Account(t, spec.Account)

	baseURL := "http://127.0.0.1" + listenAddr
	cfg := NodeConfig{
		AllowLoopback:   true,
		ListenAddr:      listenAddr,
		RegistryID:      spec.RegistryID,
		PDPBaseURL:      pdpURL,
		NATSURL:         broker.URL,
		AccountSeedFile: acct.SeedFile,
		TrustSeedFile:   broker.TrustSeedFile,
		ResolverDir:     broker.ResolverDir,
		NodeDID:         spec.NodeDID,
		ResolverBaseURL: baseURL,
		VCStoreEndpoint: baseURL,
		LoopsBlock:      spec.Loops(baseURL),
		Extra:           spec.Tunables,
	}

	nodeDir := filepath.Join(workDir, spec.Account+"-node")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	node := StartNode(t, spec.Account, bin, nodeDir, listenAddr, cfg.Render())
	waitSubscribed := func() {
		for _, subj := range spec.IngressSubjects {
			broker.WaitForSubscriber(t, subj, 30*time.Second)
		}
	}
	waitSubscribed()
	return SingleNodeEnv{
		NodeBase:  node.BaseURL,
		NATSURL:   broker.URL,
		AcctSeed:  acct.Seed,
		SinkLines: func() []string { return node.SinkLines() },
		RestartNode: func() string {
			node.Stop(t)
			node = StartNode(t, spec.Account, bin, nodeDir, listenAddr, cfg.Render())
			waitSubscribed()
			return node.BaseURL // the process runtime rebinds the same port
		},
		StopNode: func() { node.Stop(t) },
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

	seed, err := os.ReadFile(filepath.Join(testdata, spec.Account+"-account.seed"))
	if err != nil {
		t.Fatal(err)
	}
	return SingleNodeEnv{
		NodeBase:  nodeBase,
		NATSURL:   "nats://" + c.Port(t, "nats", 4222),
		AcctSeed:  strings.TrimSpace(string(seed)),
		SinkLines: func() []string { return c.SinkLines(t, spec.Account) },
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
