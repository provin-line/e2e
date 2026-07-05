# provin.e2e

End-to-end tests for the provin OSS components: black-box scenarios that run the
real `standalone` binary against a real NATS server and assert over the wire.

## Runtimes

Every scenario runs in one of two equivalent runtimes:

- **process** (default): the harness builds `cmd/standalone` from `repos/oss`,
  runs it and a real `nats-server` as local subprocesses, and drives the
  scenario over real TCP ports. No Docker required.
- **compose**: the same topology as containers via each scenario's
  `docker-compose.yml` (images built by `make docker-build`). Set
  `E2E_RUNTIME=compose`.

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
