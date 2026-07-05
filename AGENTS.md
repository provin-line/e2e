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
2. **Assertions may use oss packages** (`vc`, `did`, generated proto clients)
   to verify what came over the wire cryptographically.
3. **Both runtimes stay equivalent.** A scenario's process-mode topology and
   its `docker-compose.yml` must describe the same node/config layout. A change
   to one updates the other.
4. **Findings over workarounds.** When a scenario needs something the product
   doesn't provide (missing surface, config gap, manual step), prefer recording
   it as a finding and doing the minimal harness workaround, over silently
   building product features into the harness.
5. Scenario layout: `scenarios/<name>/{<name>_test.go, docker-compose.yml, testdata/}`.
   Shared machinery lives in `internal/harness`.
