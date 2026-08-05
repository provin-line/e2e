# Changelog

All notable changes to this repository are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A version here is a **citation, not an install target** — nothing in this
repository is published to a registry or imported by anyone, and the gates on
both sides pin by SHA rather than by tag (`provin.oss`'s `E2E_REF`, this
repository's [`pins.env`](pins.env)). What a tag names is a harness revision,
so that a claim of the form "both runtimes green against oss `<sha>`" has
something to point at. See [SECURITY.md](SECURITY.md#supported-versions).

## [Unreleased]

## [0.2.0] - 2026-08-05

The Paper 04 measurement line: the supply-chain scenario's acceptance
became exact, and `longchain` gained a number for the steady state (the
paper's §6.8.8 revision `a9600f4` is an ancestor of this tag).

### Added

- Supply-chain: exact-view delivery across three organizations with their
  own registries — a strict source-set profile beside the linear one, and
  host-readable agent-delivery records asserted from the test.
- `longchain`: depth- and rate-parameterized steady-state measurement
  under constant open-loop load.

### Fixed

- `retail-pipeline` runs as the invoking user for its writable `/app/data`
  bind mount, so container-written 0600 delivery files stay readable — and
  mode-checkable — by the host-side test on Linux.

## [0.1.0] - 2026-07-27

The first tag, cut the day the repository went public. Everything below already
existed; this records what the harness *is* at the point it became citable.

### The harness

- **11 scenarios**, each a black-box story driven over the wire against real
  binaries — `simple`, `branching`, `longchain`, `recall`, `losswindow`,
  `httpingest`, `auditsurvival`, `archiveverify`, `aggregatebundle`,
  `sensoraggregate`, `supplychain`. No scenario reaches into the node's
  internals; each asserts only over surfaces a real client has.
- **Two runtimes, both required.** `process` builds `cmd/network` and
  `cmd/pipeline` from the pinned oss revision and runs them as local
  subprocesses against an in-process `nats-server/v2`; `compose` runs the same
  scenarios in containers. They are equivalent by rule (AGENTS.md rule 3), and
  **all 11 scenarios ship both** — `TestComposeParity` fails the suite if a
  scenario has no compose twin and no ledgered deferral, so the container half
  cannot quietly rot behind a guard that never runs against it.
- **The separated topology only.** `network` (control plane) and `pipeline`
  (data plane) as two independent processes talking over the wire — the shape
  production deployments use, and the one the retired `cmd/standalone`
  collapsed into a single process.
- **Real auth.** `wireauth` is the real implementation; the policy decision
  point is stubbed allow-all (`cmd/pdpstub`) so scenarios exercise the
  enforcement path without encoding one deployment's policy.

### Evidence discipline

- [`FINDINGS.md`](FINDINGS.md) — the findings register. Every entry carries a
  rationale, a resolution condition, and provenance; the open ones say what
  would close them rather than sitting as an untriaged list. `E2E-F-030` is
  open: an ingest handle cannot be mapped to the head it produced, which is a
  missing product surface rather than a harness gap, and the register says so
  instead of the harness papering over it.
- [`pins.env`](pins.env) — the oss revision under test, by SHA, with
  `make verify-pin` asserting the checkout actually landed on it *before* the
  suites run. A checkout that quietly resolved something else would otherwise
  produce a green run attributed to a revision it never tested.
- **Both directions gated.** This repository's CI holds oss still and varies
  the harness; `provin.oss`'s own `e2e.yml` does the reverse, pinning the
  harness and running these scenarios against each of its commits. Neither
  direction can catch the other's regressions, which is why both exist.
  `oss-crosscheck.yml` additionally runs against oss HEAD nightly as a
  non-gating drift signal, and — only once that pair is proven green — reports
  when `provin.oss`'s pin has fallen behind.

### Added at the public cut

- `LICENSE` (Apache-2.0) and `SECURITY.md`. The security policy is mostly a
  routing document: a flaw in the *system under test* belongs in `provin.oss`
  or `provin.auth`, and reading these scenarios is a reasonable way to notice
  one.

### Changed at the public cut

- Both cross-repo checkouts are anonymous. They previously minted a GitHub App
  installation token, because `GITHUB_TOKEN` cannot read a private sibling and
  the organisation disables deploy keys; publishing `provin.oss` removed the
  constraint, and with it the App, the `CI_APP_ID` variable and the
  `CI_APP_PRIVATE_KEY` secret. Two workflows that handled a private key now
  handle none.

[Unreleased]: https://github.com/provin-line/e2e/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/provin-line/e2e/releases/tag/v0.1.0
