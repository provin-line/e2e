package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nkeys"

	natsop "github.com/provin-line/oss/network/pkg/services/chainmanager/infra/nats"
)

// NATS is a real nats-server in operator mode plus the provisioned trust root
// and accounts, mirroring a production deployment's out-of-band provisioning:
// operator + account nkeys generated ahead of time, account-claims JWTs
// published to a resolver directory, the broker trusting the operator key.
//
// The broker runs in the harness process (a real TCP listener); the nodes under
// test reach it only via its client URL, so they stay black-box.
type NATS struct {
	URL           string
	TrustSeedFile string
	ResolverDir   string

	server   *server.Server
	resolver *server.MemAccResolver
	accounts map[string]*NATSAccount
}

// NATSAccount is one provisioned NATS account and its chain-manager operator
// (the JWT-mutating side of grants).
type NATSAccount struct {
	Name     string
	SeedFile string
	Seed     string
	Pub      string
	Op       *natsop.Operator
}

// StartNATS provisions an operator trust root plus one account per name, writes
// the seed files and account JWTs under dir, and starts the broker. Grants
// (cross-account export/import) can be added afterwards with Grant — before
// the granted client connects (cold-account ordering).
func StartNATS(t *testing.T, dir string, accountNames ...string) *NATS {
	t.Helper()

	op, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatalf("nats: create operator: %v", err)
	}
	opSeed, _ := op.Seed()
	opPub, _ := op.PublicKey()

	resolverDir := filepath.Join(dir, "jwts")
	if err := os.MkdirAll(resolverDir, 0o755); err != nil {
		t.Fatalf("nats: mkdir resolver dir: %v", err)
	}
	trustSeedFile := filepath.Join(dir, "operator.seed")
	if err := os.WriteFile(trustSeedFile, opSeed, 0o600); err != nil {
		t.Fatalf("nats: write operator seed: %v", err)
	}

	n := &NATS{
		TrustSeedFile: trustSeedFile,
		ResolverDir:   resolverDir,
		resolver:      &server.MemAccResolver{},
		accounts:      map[string]*NATSAccount{},
	}

	for _, name := range accountNames {
		acc, err := nkeys.CreateAccount()
		if err != nil {
			t.Fatalf("nats: create account %s: %v", name, err)
		}
		accSeed, _ := acc.Seed()
		accPub, _ := acc.PublicKey()
		seedFile := filepath.Join(dir, name+"-account.seed")
		if err := os.WriteFile(seedFile, accSeed, 0o600); err != nil {
			t.Fatalf("nats: write account seed %s: %v", name, err)
		}
		aop, err := natsop.New(natsop.Config{
			AccountSeed:   string(accSeed),
			TrustRootSeed: string(opSeed),
			URL:           "nats://unused-in-e2e:4222",
			Publisher:     natsop.NewDirPublisher(resolverDir),
		})
		if err != nil {
			t.Fatalf("nats: natsop.New(%s): %v", name, err)
		}
		// Account JWTs are written only on a mutation; a bootstrap export makes
		// the bare account connectable before any real grant exists.
		if _, err := aop.AddExport("chain.bootstrap." + name); err != nil {
			t.Fatalf("nats: bootstrap export %s: %v", name, err)
		}
		n.accounts[name] = &NATSAccount{Name: name, SeedFile: seedFile, Seed: string(accSeed), Pub: accPub, Op: aop}
	}

	n.bridge(t)
	n.server = natstest.RunServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		TrustedKeys: []string{opPub}, AccountResolver: n.resolver,
	})
	t.Cleanup(n.server.Shutdown)
	n.URL = n.server.ClientURL()
	return n
}

// Account returns the provisioned account by name.
func (n *NATS) Account(t *testing.T, name string) *NATSAccount {
	t.Helper()
	a, ok := n.accounts[name]
	if !ok {
		t.Fatalf("nats: unknown account %q", name)
	}
	return a
}

// Grant establishes a cross-account subject grant: AddExport on the exporter,
// AddImport on the importer, then re-bridges the updated JWTs into the broker's
// resolver AND pushes them into any already-loaded account. The push matters:
// the broker caches account claims on first lookup and (with non-expiring
// JWTs) never re-fetches from the resolver, so without UpdateAccountClaims a
// grant issued after either side connected would silently never take effect.
func (n *NATS) Grant(t *testing.T, exporter, importer, subject string) {
	t.Helper()
	exp := n.Account(t, exporter)
	imp := n.Account(t, importer)
	if _, err := exp.Op.AddExport(subject); err != nil {
		t.Fatalf("nats: AddExport(%s, %s): %v", exporter, subject, err)
	}
	if err := imp.Op.AddImport(subject, exp.Pub, subject); err != nil {
		t.Fatalf("nats: AddImport(%s, %s): %v", importer, subject, err)
	}
	n.bridge(t)
	n.refreshLoaded(t, exp.Pub)
	n.refreshLoaded(t, imp.Pub)
}

// refreshLoaded pushes the resolver-dir JWT for pub into the broker's live
// account object, if the broker has already loaded that account.
func (n *NATS) refreshLoaded(t *testing.T, pub string) {
	t.Helper()
	acc, err := n.server.LookupAccount(pub)
	if err != nil || acc == nil {
		return // not loaded yet — the resolver copy is authoritative at first connect
	}
	raw, err := os.ReadFile(filepath.Join(n.ResolverDir, pub+".jwt"))
	if err != nil {
		t.Fatalf("nats: read refreshed jwt for %s: %v", pub, err)
	}
	claims, err := jwt.DecodeAccountClaims(string(raw))
	if err != nil {
		t.Fatalf("nats: decode refreshed jwt for %s: %v", pub, err)
	}
	n.server.UpdateAccountClaims(acc, claims)
}

// bridge loads every <accountPub>.jwt the operators wrote into the broker's
// account resolver (idempotent overwrite).
func (n *NATS) bridge(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(n.ResolverDir)
	if err != nil {
		t.Fatalf("nats: read resolver dir: %v", err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".jwt") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(n.ResolverDir, e.Name()))
		if err != nil {
			t.Fatalf("nats: read jwt %s: %v", e.Name(), err)
		}
		if err := n.resolver.Store(strings.TrimSuffix(e.Name(), ".jwt"), string(b)); err != nil {
			t.Fatalf("nats: resolver store %s: %v", e.Name(), err)
		}
	}
}
