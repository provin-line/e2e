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

Note: `make clone` fetches `repos/oss` from GitHub; a local development
checkout may instead symlink `repos/oss` to a working copy, in which case the
tested oss revision is whatever that working copy holds.

## Setup

```bash
make clone         # Clone or update dependency repos (oss)
make test          # Run all scenarios (process runtime)
make docker-build  # Build Docker images from cloned repos (compose runtime)
```

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
└── Makefile
```

`FINDINGS.md` records what a scenario needed and the product did not provide,
and it is the only place a missing compose twin can be licensed from — a
`t.Skip` is not a record, because the suite exits 0 on a skip.

Scenario assertions are plain Go tests that import the oss module (via a local
`replace` to `repos/oss`), so credentials fetched over the wire are verified
with the same `vc` packages the product uses.
