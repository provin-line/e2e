package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	natsop "github.com/provin-line/oss/network/pkg/services/chainmanager/infra/nats"
)

// ComposeRuntime reports whether the compose runtime was requested
// (E2E_RUNTIME=compose). The process runtime is the default.
func ComposeRuntime() bool { return os.Getenv("E2E_RUNTIME") == "compose" }

// ComposeProvision is the pre-boot provisioning for a compose scenario: NATS
// trust root + accounts as files, account-claims JWTs, the operator JWT, and a
// nats-server.conf preloading every account — the same out-of-band artifacts a
// production operator prepares, laid out under testdata/ for volume mounts.
type ComposeProvision struct {
	Dir      string // testdata root (mounted into containers)
	accounts map[string]*NATSAccount
	// brokerConfigWritten latches WriteBrokerConfig: the compose broker
	// preloads static claims, so a Grant after the render silently never
	// takes effect — fail loud instead.
	brokerConfigWritten bool
}

// ProvisionCompose writes the NATS provisioning artifacts under dir:
//
//	dir/operator.seed            trust-root nkey seed (mounted to nodes)
//	dir/<name>-account.seed      per-account nkey seeds (mounted to nodes)
//	dir/jwts/<pub>.jwt           account-claims JWTs (DirPublisher output)
//	dir/nats/operator.jwt        self-signed operator JWT
//	dir/nats/nats-server.conf    operator-mode config preloading all accounts
//
// Grants must be added (Grant) BEFORE WriteBrokerConfig renders the conf —
// compose brokers preload account claims statically at boot.
func ProvisionCompose(t *testing.T, dir string, accountNames ...string) *ComposeProvision {
	t.Helper()
	op, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatalf("compose: create operator: %v", err)
	}
	opSeed, _ := op.Seed()
	opPub, _ := op.PublicKey()

	if err := os.MkdirAll(filepath.Join(dir, "jwts"), 0o755); err != nil {
		t.Fatalf("compose: mkdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "nats"), 0o755); err != nil {
		t.Fatalf("compose: mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "operator.seed"), opSeed, 0o644); err != nil {
		t.Fatalf("compose: write operator seed: %v", err)
	}

	// Self-signed operator JWT — the broker's trust anchor.
	opClaims := jwt.NewOperatorClaims(opPub)
	opClaims.Name = "provin-e2e"
	opJWT, err := opClaims.Encode(op)
	if err != nil {
		t.Fatalf("compose: encode operator jwt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nats", "operator.jwt"), []byte(opJWT), 0o644); err != nil {
		t.Fatalf("compose: write operator jwt: %v", err)
	}

	p := &ComposeProvision{Dir: dir, accounts: map[string]*NATSAccount{}}
	for _, name := range accountNames {
		acc, err := nkeys.CreateAccount()
		if err != nil {
			t.Fatalf("compose: create account %s: %v", name, err)
		}
		accSeed, _ := acc.Seed()
		accPub, _ := acc.PublicKey()
		seedFile := filepath.Join(dir, name+"-account.seed")
		if err := os.WriteFile(seedFile, accSeed, 0o644); err != nil {
			t.Fatalf("compose: write account seed %s: %v", name, err)
		}
		aop, err := natsop.New(natsop.Config{
			AccountSeed:   string(accSeed),
			TrustRootSeed: string(opSeed),
			URL:           "nats://unused-in-e2e:4222",
			Publisher:     natsop.NewDirPublisher(filepath.Join(dir, "jwts")),
		})
		if err != nil {
			t.Fatalf("compose: natsop.New(%s): %v", name, err)
		}
		if _, err := aop.AddExport("chain.bootstrap." + name); err != nil {
			t.Fatalf("compose: bootstrap export %s: %v", name, err)
		}
		p.accounts[name] = &NATSAccount{Name: name, SeedFile: seedFile, Seed: string(accSeed), Pub: accPub, Op: aop}
	}
	return p
}

// Account returns a provisioned account by name.
func (p *ComposeProvision) Account(t *testing.T, name string) *NATSAccount {
	t.Helper()
	a, ok := p.accounts[name]
	if !ok {
		t.Fatalf("compose: unknown account %q", name)
	}
	return a
}

// Grant adds a cross-account export/import pair. Call before WriteBrokerConfig.
func (p *ComposeProvision) Grant(t *testing.T, exporter, importer, subject string) {
	t.Helper()
	if p.brokerConfigWritten {
		t.Fatalf("compose: Grant(%s->%s) after WriteBrokerConfig — the broker preloads static claims; grant all subjects BEFORE rendering the broker config", exporter, importer)
	}
	exp := p.Account(t, exporter)
	imp := p.Account(t, importer)
	if _, err := exp.Op.AddExport(subject); err != nil {
		t.Fatalf("compose: AddExport(%s, %s): %v", exporter, subject, err)
	}
	if err := imp.Op.AddImport(subject, exp.Pub, subject); err != nil {
		t.Fatalf("compose: AddImport(%s, %s): %v", importer, subject, err)
	}
}

// WriteBrokerConfig renders nats-server.conf with every account's current JWT
// preloaded (memory resolver: static claims, grants fixed at boot).
func (p *ComposeProvision) WriteBrokerConfig(t *testing.T) {
	t.Helper()
	p.brokerConfigWritten = true
	var b strings.Builder
	b.WriteString("port: 4222\nhttp: 8222\n")
	b.WriteString("operator: /etc/nats/operator.jwt\n")
	b.WriteString("resolver: MEMORY\nresolver_preload: {\n")
	for _, a := range p.accounts {
		raw, err := os.ReadFile(filepath.Join(p.Dir, "jwts", a.Pub+".jwt"))
		if err != nil {
			t.Fatalf("compose: read jwt for %s: %v", a.Name, err)
		}
		fmt.Fprintf(&b, "  %s: %q\n", a.Pub, strings.TrimSpace(string(raw)))
	}
	b.WriteString("}\n")
	if err := os.WriteFile(filepath.Join(p.Dir, "nats", "nats-server.conf"), []byte(b.String()), 0o644); err != nil {
		t.Fatalf("compose: write nats-server.conf: %v", err)
	}
}

// WriteNodeConfig renders a node's application.conf under
// dir/<node>/config/application.conf for the compose volume mount.
func (p *ComposeProvision) WriteNodeConfig(t *testing.T, node string, cfg NodeConfig) {
	t.Helper()
	confDir := filepath.Join(p.Dir, node, "config")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatalf("compose: mkdir node config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(confDir, "application.conf"), []byte(cfg.Render()), 0o644); err != nil {
		t.Fatalf("compose: write node config: %v", err)
	}
}

// Compose is one running docker-compose project.
type Compose struct {
	Project string
	File    string
	dir     string // working directory for compose commands
}

// ComposeUp starts the scenario's compose file and registers teardown
// (down -v). The project name is derived from the scenario directory:
// DETERMINISTIC per checkout+scenario — a stack leaked by a hard-killed run
// (no Cleanup) is reclaimed by the pre-up down of the next run — while the
// path-hash suffix keeps two checkouts sharing one Docker daemon from tearing
// each other's stacks down. Concurrent same-scenario runs on one checkout are
// unsupported either way (they'd clobber the shared testdata/).
func ComposeUp(t *testing.T, scenarioDir string) *Compose {
	t.Helper()
	sum := sha256.Sum256([]byte(scenarioDir))
	project := "e2e-" + filepath.Base(scenarioDir) + "-" + hex.EncodeToString(sum[:4])
	c := &Compose{Project: project, File: filepath.Join(scenarioDir, "docker-compose.yml"), dir: scenarioDir}
	down := func(logf func(format string, args ...any)) {
		cmd := exec.Command("docker", "compose", "-p", c.Project, "-f", c.File, "down", "-v", "--remove-orphans")
		cmd.Dir = c.dir
		if out, err := cmd.CombinedOutput(); err != nil {
			logf("compose down: %v\n%s", err, out)
		}
	}
	down(t.Logf) // reclaim a stack a hard-killed previous run may have leaked
	t.Cleanup(func() { down(t.Logf) })
	cmd := exec.Command("docker", "compose", "-p", c.Project, "-f", c.File, "up", "-d")
	cmd.Dir = c.dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compose up: %v\n%s", err, out)
	}
	return c
}

// Port returns the host-published address ("127.0.0.1:<hostport>") for a
// service's container port. It retries briefly: a restarting service (e.g. a
// node that failed closed before its broker accepted connections) has no
// published mapping until it is running again.
func (c *Compose) Port(t *testing.T, service string, containerPort int) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "compose", "-p", c.Project, "-f", c.File,
			"port", service, strconv.Itoa(containerPort))
		cmd.Dir = c.dir
		out, err := cmd.Output()
		addr := strings.TrimSpace(string(out))
		if err == nil && addr != "" {
			// docker may print 0.0.0.0:PORT — normalize to a dialable loopback host.
			if strings.HasPrefix(addr, "0.0.0.0:") {
				addr = "127.0.0.1:" + strings.TrimPrefix(addr, "0.0.0.0:")
			}
			if strings.HasPrefix(addr, "[::]:") {
				addr = "127.0.0.1:" + strings.TrimPrefix(addr, "[::]:")
			}
			return addr
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("compose port %s %d: no published port after 60s (last error: %v)\n--- %s logs ---\n%s",
		service, containerPort, lastErr, service, c.Logs(t, service))
	return ""
}

// Logs returns a service's accumulated log output (without the compose
// "service-1 |" prefixes, since it targets one service).
func (c *Compose) Logs(t *testing.T, service string) string {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", c.Project, "-f", c.File,
		"logs", "--no-color", "--no-log-prefix", service)
	cmd.Dir = c.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compose logs %s: %v\n%s", service, err, out)
	}
	return string(out)
}

// SinkLines extracts NDJSON-looking lines from a service's logs (the compose
// twin of Node.SinkLines).
func (c *Compose) SinkLines(t *testing.T, service string) []string {
	t.Helper()
	var lines []string
	for _, line := range strings.Split(c.Logs(t, service), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "{") {
			lines = append(lines, line)
		}
	}
	return lines
}

// WaitHTTPHealthy polls an http URL until it returns 200 (compose services
// publish ephemeral host ports, so health is observed from the host side).
func WaitHTTPHealthy(t *testing.T, name, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("compose service %s never became healthy at %s", name, url)
}

// WaitForSubscriberHTTP is WaitForSubscriber's compose twin: it polls the
// broker's monitoring endpoint (/subsz?subs=1) until subject has a subscriber.
func WaitForSubscriberHTTP(t *testing.T, monitorBase, subject string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(monitorBase + "/subsz?subs=1")
		if err == nil {
			body, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if rerr == nil && strings.Contains(string(body), `"`+subject+`"`) {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("compose: no subscriber on %q after %s", subject, timeout)
}
