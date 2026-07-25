# provin.e2e — Development Guidelines

Black-box end-to-end tests for provin OSS. This repo orchestrates real binaries
(process runtime) or containers (compose runtime); it must never reach into
`repos/oss` internals except by importing its published Go packages.

## Rules

1. **Scenarios are black-box.** A scenario may only touch a node through its
   real surfaces: ConnectRPC endpoints, the public DID resolution route, NATS
   subjects with account credentials, stdout (NDJSON sink), and config files.
   No in-process construction of node components — that's what `repos/oss`'s
   own tests do.
2. **Assertions and out-of-band setup may use published oss packages** —
   `vc`/`did`/generated proto clients for cryptographic verification of what
   came over the wire, plus client-side/provisioning packages for the roles a
   deployment performs out-of-band: NATS entity provisioning
   (`network/pkg/services/chainmanager/infra/nats`), DID resolution as a
   relying party (`network/pkg/didresolver`, `network/pkg/core` guard), and
   producer stimulus (`pipeline/transport/nats`). What stays forbidden is
   constructing the node's own internals (data planes, handlers, stores)
   in-process.
3. **Both runtimes stay equivalent.** A scenario's process-mode topology and
   its `docker-compose.yml` must describe the same node/config layout. A change
   to one updates the other. Every scenario ships both. A missing
   `docker-compose.yml` is a rule violation unless an **open** finding in
   `FINDINGS.md` licenses it through a `**Compose twin deferred**` field naming
   that scenario; `TestComposeParity` fails the suite for any scenario that
   lacks a twin without such a licence, and for any scenario that has a twin
   while its licence is still open. A `t.Skip` is not a record — `go test` exits
   0 on a skip — so the register and that test are the whole of the enforcement.
   Both `make test` and `make test-compose` therefore run `./...`, not
   `./scenarios/...`: Go does not run a `_test.go` from an imported dependency,
   and the guard lives in `internal/harness`.

   The original blanket exemption — deferral permitted while the compose
   runtime itself was pending — expired with `E2E-F-018`, which closed when
   every then-existing scenario gained a twin. Do not reach for it again.
4. **Findings over workarounds.** When a scenario needs something the product
   doesn't provide (missing surface, config gap, manual step), prefer recording
   it as a finding in `FINDINGS.md` and doing the minimal harness workaround,
   over silently building product features into the harness. Findings get
   namespaced IDs (`E2E-F-NNN`); cite the ID, not a bare `#N`, so a reference
   from another repository still resolves.
5. Scenario layout: `scenarios/<name>/{<name>_test.go, docker-compose.yml, testdata/}`.
   Shared machinery lives in `internal/harness`.
