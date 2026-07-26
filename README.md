# provin.e2e

End-to-end tests for the provin OSS components: black-box scenarios that run the
real `network` + `pipeline` binaries against a real NATS server and assert over
the wire.

## Runtimes

Every scenario runs in one of two equivalent runtimes:

- **process** (default): the harness builds `cmd/network` and `cmd/pipeline`
  from `repos/oss`, runs them and a real `nats-server` as local subprocesses,
  and drives the scenario over real TCP ports. No Docker required.
- **compose**: the same topology as containers (`E2E_RUNTIME=compose`).
  Provisioning artifacts (NATS operator/account seeds, account JWTs, broker
  config, node configs) are generated into the scenario's `testdata/` by the
  test itself; images come from `make docker-build`. Every scenario runs in
  both runtimes (`make test-compose`); `TestComposeParity` fails the suite if
  one stops doing so without an open finding recording why.

Each node boots with its production fail-closed auth wiring intact, but the PDP
behind it is `cmd/pdpstub` (allow-all) and the bearer is a fixed harness
constant: what these scenarios cover is the node's own credential gate and its
wireauth-signed peer calls, never JWT issuance or a policy-decision deny. The
real three-layer auth stack (auth.provider + o3co policy-verifier) is exercised
by `repos/oss`'s `deploy/quickstart`, not from here.

## The oss revision under test

`repos/oss` is an **input**, not something a build step maintains. `pins.env`
names the revision by SHA, `make clone` fetches exactly that when `repos/oss` is
absent, and nothing ever advances a checkout that already exists — a silent
`git pull` would make a green run unattributable to any revision, and would
rewrite a development checkout out from under whoever is working in it.

```bash
make clone         # fetch repos/oss at the pinned revision (no-op if present)
make checkout-oss  # move an existing checkout onto the pin (after editing it)
make verify-pin    # fail unless repos/oss is exactly at the pin — CI's preflight
make test          # all scenarios + harness tests (process runtime)
make docker-build  # build the images the compose runtime needs
make test-compose  # the same suite in containers
```

A local development checkout may instead symlink `repos/oss` at a working copy,
in which case the tested revision is whatever that copy holds and `verify-pin`
fails by design — `pins.env` cannot vouch for a symlink's contents.

## CI

`.github/workflows/ci.yml` runs both runtimes against the pinned oss revision on
every pull request. It gates one direction only: **an e2e change breaking
against known-good oss.** The direction that matters more — an oss change
breaking these scenarios — cannot be gated from here, because nothing in this
repository runs when oss changes; that job lives in `provin.oss` and pins a
revision of this repo.

`oss-crosscheck.yml` runs the same suites against oss HEAD nightly and is
deliberately **not** a gate: a red run there means the pair has drifted, not
that either side is broken, and which side moves is a human call.

Both cross-repo checkouts mint a short-lived GitHub App installation token,
scoped to the single repository being read. A workflow's own `GITHUB_TOKEN` is
scoped to its own repository and cannot read a private sibling, and this
organisation disables deploy keys by policy — Apps are what it recommends
instead. Setup is one App plus two settings (`CI_APP_ID` variable,
`CI_APP_PRIVATE_KEY` secret), which may live at the organisation level and serve
both repositories at once.

## Structure

```text
e2e/
├── repos/                 ← Cloned dependency repos (.gitignore'd)
│   └── oss/               ← provin-line/oss
├── scenarios/
│   ├── simple/            ← single-org source→chained→sink pipeline story
│   ├── branching/         ← fan-out + complementary-filter delivery matrix
│   ├── longchain/         ← 10-hop relay, wire chain walk, deep audit
│   ├── sensoraggregate/   ← aggregate window fold + source-commitment audits
│   ├── supplychain/       ← three orgs, own registries, cross-org grants
│   ├── httpingest/        ← ingestion over the apipush HTTP surface
│   ├── recall/            ← forward descendant walk + audit verdicts (discovery RPCs)
│   ├── losswindow/        ← at-most-once loss named via the signed emission log
│   ├── auditsurvival/     ← restart: identity AND evidence survive (flipped canary)
│   ├── archiveverify/     ← offline chain re-verification after infra death
│   └── aggregatebundle/   ← aggregate-complete bundle: source commitment travels
├── cmd/pdpstub/           ← allow-all policy-verifier (PDP) stub for scenarios
├── internal/harness/      ← provisioning + node lifecycle + assertion helpers
├── FINDINGS.md            ← the findings register (AGENTS.md rules 3 and 4)
├── pins.env               ← the oss revision the suites are verified against
├── .github/workflows/     ← ci (pinned, gating) + oss-crosscheck (HEAD, advisory)
└── Makefile
```

`FINDINGS.md` records what a scenario needed and the product did not provide,
and it is the only place a missing compose twin can be licensed from — a
`t.Skip` is not a record, because the suite exits 0 on a skip.

Scenario assertions are plain Go tests that import the oss module (via a local
`replace` to `repos/oss`), so credentials fetched over the wire are verified
with the same `vc` packages the product uses.
