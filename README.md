# provin.e2e

End-to-end tests for the provin OSS components: black-box scenarios that run the
real `standalone` binary against a real NATS server and assert over the wire.

## Runtimes

Every scenario runs in one of two equivalent runtimes:

- **process** (default): the harness builds `cmd/standalone` from `repos/oss`,
  runs it and a real `nats-server` as local subprocesses, and drives the
  scenario over real TCP ports. No Docker required.
- **compose**: the same topology as containers (`E2E_RUNTIME=compose`).
  Provisioning artifacts (NATS operator/account seeds, account JWTs, broker
  config, node configs) are generated into the scenario's `testdata/` by the
  test itself; images come from `make docker-build`. All scenarios run in
  both runtimes (`make test-compose`).

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

```
e2e/
├── repos/                 ← Cloned dependency repos (.gitignore'd)
│   └── oss/               ← provin-line/oss
├── scenarios/
│   └── simple/            ← single-org source→chained→sink pipeline story
├── cmd/pdpstub/           ← allow-all policy-verifier (PDP) stub for scenarios
├── internal/harness/      ← provisioning + node lifecycle + assertion helpers
└── Makefile
```

Scenario assertions are plain Go tests that import the oss module (via a local
`replace` to `repos/oss`), so credentials fetched over the wire are verified
with the same `vc` packages the product uses.
