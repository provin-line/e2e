# provin.e2e findings register

A **finding** is something a scenario needed that the product did not provide —
a missing surface, a config gap, a manual step — or a deliberate gap in this
harness's own coverage. AGENTS.md rule 4 prefers recording a finding over
silently building the product feature into the harness; rule 3 requires that a
deferred compose twin be recorded here rather than left to a bare `t.Skip`.

This file is the register. It is authoritative, it lives in this repository so
that a rule, its exception, and the test that resolves the exception travel
together through clones, branches and CI, and it is the file rule 3 names.

## Why most of this file is reconstructed

Findings were numbered `#N` from the start and cited by number from code
comments in this repository, from `provin.oss` source, and from a spec document
in a sibling agent scope — but the register itself was never committed. It
existed only in agent session context, and was lost. Of a series reaching at
least 28, fourteen numbers are recoverable from surviving citations and commit
messages; the rest are gone.

The repair follows two rules:

- **Backfill only what a surviving artifact supports.** Every reconstructed
  entry cites its provenance. Nothing here is inferred from a number's
  neighbours.
- **Never reuse a lost number.** Unrecovered numbers stay reserved (below), not
  recycled, because external references to them may still exist.

## IDs

Every finding has a namespaced ID: `E2E-F-NNN`, where `NNN` is the original bare
number for reconstructed entries (`#18` → `E2E-F-018`) and the next free number
for new ones. Cite the ID, never a bare `#N`.

The namespace is not decoration. `provin.oss` carries its own, unrelated `#N`
code-review series in the same source tree — its `finding #2` is an fsync defect
in the mirror store, nothing to do with this register's `E2E-F-002` (HTTP
ingestion). A bare `#2` in a shared tree is genuinely ambiguous, and was already
being read as if it were not.

## Required fields

An open finding carries all of: **ID**, **title**, **status**, **affected
scenario(s)**, **rationale** (why the product/harness gap is real), **workaround**
(what the harness does meanwhile), **resolution condition** (what must become
true to close it), **provenance**. A resolved finding adds **resolution
evidence** — the commit, PR, or test that closes it.

Two fields are machine-read by `TestComposeParity`, so their form is fixed
rather than stylistic:

- `- **Status**: open` — the value must be exactly `open` or `resolved`, with no
  qualifier. A variant like `open (blocked)` reads as not-open, which makes the
  guard demand any compose twin the finding was meant to license.
- `- **Compose twin deferred**: scenarios/<name>` — **only** this field licenses
  a scenario to ship without a `docker-compose.yml`. Several comma-separated
  paths work; keep them on one line (a wrapped continuation is not read, which
  fails safe). Add it only to a finding that IS a compose-twin deferral.

`**Affected scenario(s)**` is required prose and is deliberately **not** read as
a licence. Most findings are product gaps that merely affect some scenario, and
treating those as compose licences would set rule 4 against rule 3: recording an
ordinary finding against a scenario that already has a twin would fail the
suite.

## Open findings

### E2E-F-029 — `losswindow`'s delivery waits assert cardinality, not identity

- **Status**: open
- **Affected scenario**: `scenarios/losswindow`
- **Rationale**: `waitDelivered` counts DISTINCT credential hashes, so "sink
  delivery #4 after recovery" is satisfied by any second distinct hash rather
  than by `#4` specifically. The harness's own guidance for this exact runtime
  divergence (`SingleNodeEnv.RestartNode`'s doc: "match sink records with
  payloads distinct per phase") prescribes matching on payload, and
  `scenarios/auditsurvival` already does. Cardinality and identity coincide
  today because core NATS is at-most-once with no redelivery path — so this is
  not a live false pass. They stop coinciding the moment a redelivery path
  exists (JetStream, a sink retry): `#2` landing late while `#4` is dropped
  would satisfy the wait, and the run would then fail further down as
  `lost sequences = [3], want [2,3]` — a delivery bug misreported as a
  loss-accounting bug.
- **Workaround**: exact-equality distinct counts over the union of a pre-outage
  snapshot and current sink output. Verified load-bearing by mutation (with the
  consumer outage disabled the run fails), and the arithmetic is correct in both
  runtimes — but it proves a weaker property than the assertion's own name.
- **Resolution condition**: `deliveredHashes` parses the sink record's payload,
  and the three waits assert reading SETS (`{1}`, `{1,4}`, `{1,4,5}`), with the
  hash set retained for reconciliation.
- **Provenance**: multi-agent review of the E2E-F-028 slice, 2026-07-25 (raised
  independently of the compose-twin work, on the same scenario).

## Closed findings

### E2E-F-028 — `losswindow` has no compose twin

- **Status**: resolved
- **Affected scenario**: `scenarios/losswindow`
- **Compose twin deferred**: `scenarios/losswindow` (historical — inert now that
  the status is resolved; kept so the record shows what was licensed)
- **Rationale**: rule 3 permits deferring a compose twin *while the compose
  runtime itself is pending*. That era ended: `E2E-F-018` closed it when all
  then-existing scenarios gained twins, and `make test-compose` has run the full
  suite ever since. `losswindow` reopened the exemption afterwards, so the
  clause it relies on no longer applies. The scenario's stated justification —
  that the loss-window mechanics are runtime-independent because they rest on
  subprocess stop/start — does not hold for its second half: the producer's
  checkpoint is served by the **network** process from a mirrored copy while the
  source log lives in the **pipeline** process, so durability and
  sequence-continuity across restart cross a container boundary that the
  process runtime never exercises. `auditsurvival` already demonstrates that
  restart coverage under compose is both expected and supported.
- **Workaround** (while open): `t.Skip` under `E2E_RUNTIME=compose`. The process
  runtime covered the scenario; compose coverage was simply absent, so the suite
  reported 10/11 + 1 skip as a green run.
- **Resolution condition**: a `scenarios/losswindow/docker-compose.yml`
  describing the same two-org topology as the process path, the skip removed,
  and the scenario green in both runtimes.
- **Provenance**: `scenarios/losswindow/losswindow_test.go` (the skip and its
  "recorded follow-up" comment, which pointed at this then-nonexistent file);
  commit `d8b687f`.
- **Resolution evidence**: `scenarios/losswindow/docker-compose.yml` (two orgs,
  four node services, mirroring `supplychain`'s split-topology pattern); the
  scenario refactored onto a runtime-neutral `lwEnv` seam with
  `setupProcess`/`setupCompose`; `Compose.StartService` added, since an outage
  window needs stop-then-start and `RestartService` cannot express "stay down".
  Green in both runtimes.

  Verified load-bearing rather than merely green: with the compose outage
  disabled as a mutation, the run FAILS — `#2`/`#3` get delivered, so the
  distinct-delivery count never reaches the expected 2 and the wait times out.
  That mutation also exposed why the scenario's original "at least one
  delivered" check was too weak to be a twin's gate at all: it passes under
  compose on `#1`'s log line alone, because `docker logs` accumulate across a
  container restart where the process runtime starts a fresh stream. The check
  now counts DISTINCT credentials over the union of a pre-outage snapshot and
  current output, in both runtimes. (That check is still cardinality rather than
  identity — `E2E-F-029`.)

  The producer restart takes both planes down together (pipeline→network down,
  network→pipeline up), matching the process path's `SeparatedNode.Stop`. A
  rolling restart would have left the pipeline up throughout, never presenting
  it with a cold registry — which is precisely the durability case this finding
  justifies the twin on.

  Two assertion defects in the process runtime were fixed in the same pass,
  both runtime-independent and both live before this finding was opened:
  sequence continuity was never actually asserted (a post-restart emission
  forking back to `1` left the loss set at exactly `[2 3]`, identical to a
  healthy log — see `harness.ReconcileEmissionLog`'s doc), and reconciliation
  could race `#5`'s delivery because the test waited only for the emission log
  to reach size 5, not for the sink to receive it.
- **Enforcement added**: `TestComposeParity`
  (`internal/harness/parity_test.go`) fails the suite for any scenario without a
  twin that no open finding licenses, and for any scenario that has a twin while
  its licence is still open. Rule 3 names both this file and that test, and both
  `make` targets run `./...` so the guard is actually executed — scoping them to
  `./scenarios/...` had meant Go never ran it.

### Reconstructed history

Status and subject are as attested by the cited artifact. "Lifted" is the
wording the source used where it differs from "resolved".

| ID | Originally cited as | Subject | Status | Provenance |
| ----------- | --- | ------- | ------ | ---------- |
| `E2E-F-002` | `#2` | Data sources that speak HTTP but not NATS had no ingestion path; the apipush surface was the follow-up | resolved by the apipush surface, driven by `scenarios/httpingest` | `scenarios/httpingest/httpingest_test.go:3`; commit `4dc3e26` |
| `E2E-F-012` | `#12` | No per-registry resolution mapping, which blocked per-org signing nodes | lifted once oss shipped per-registry resolution (`registry-base-urls`) | commit `2ebdd45`; the scope note in an earlier commit of the same series |
| `E2E-F-013` | `#13` | Local-keystore-only signing — no remote SignerService config path | **ambiguous**: `2ebdd45`'s subject calls it lifted (node-local keys suffice for the three-org topology), but a sibling scope still cites it as an undelivered prerequisite for signing delegation. Not closed here on conflicting evidence. | commit `2ebdd45`; `scopes/spec.provin4ai/docs/provin4ai-definition-2026-07-06.md:119` |
| `E2E-F-014` | `#14` | A bare NATS account was not connectable until some grant existed | resolved in oss; the harness publishes the bare account's claims | `internal/harness/nats.go:96`; three oss sites (below) |
| `E2E-F-017` | `#17` | No wire surface for an aggregate's consumed source set; the manifest payload was the only substitute | resolved by `GetConsumedSources` | `scenarios/sensoraggregate/sensoraggregate_test.go:258`; commit `93c9474` |
| `E2E-F-018` | `#18` | Compose twins deferred for the then-existing scenarios while the compose runtime was pending | resolved — all of them gained twins, "rule 3 satisfied" | opened in commit `2cce17b`, resolved in commit `0937445` |
| `E2E-F-019` | `#19` | Container networks needed an explicit SSRF opt-in | resolved by oss `allow-private-networks` plus a process-mode loopback opt-in | commit `2cce17b` |
| `E2E-F-020` | `#20` | `depends_on` orders start only; a node dialing NATS at boot fails closed if the broker is not accepting yet | resolved by `restart: on-failure` in the compose topologies | commit `2cce17b` |
| `E2E-F-021` | `#21` | A consuming-only node with an empty bearer had its cross-node fetches rejected before the PDP | resolved — `vc-store-bearer` is rendered unconditionally; oss docs updated separately | commit `2ebdd45` |
| `E2E-F-023` | `#23` | Evidence durability: a restart must not erase audit evidence | resolved in oss (`vcresolver`/`auditor` filestores) | **no citation in this repository** — three oss sites (below); subject confirmed by commit `c9bdff2` |
| `E2E-F-024` | `#24` | The product defined no snapshot/export convention, so `archiveverify` had to invent its archive format | resolved by the audit bundle (`provin bundle export` / `verify`), oss `440fea2` | `scenarios/archiveverify/archiveverify_test.go:16`; commit `c9bdff2` |
| `E2E-F-025` | `#25` | The forward direction (a credential's descendants) existed on no API | resolved by `VCResolverService.ListSuccessors` | `scenarios/recall/recall_test.go:8`; commit `93c9474` |
| `E2E-F-026` | `#26` | Chain heads could only be learned by scraping sink stdout | resolved by `AuditService.ListAuditStatuses` | `scenarios/recall/recall_test.go:9`; commit `93c9474` |
| `E2E-F-027` | `#27` | At-most-once loss was silent: no durable, signed record of what was emitted | resolved by `dplaax.tlog.v1.TlogService` | `scenarios/losswindow/losswindow_test.go:2`; commit `d8b687f` |

Two findings from the separated-topology work were recorded in prose without
numbers and are noted here rather than assigned IDs retroactively: a fresh
deployment's first emission racing its own wireauth boot epoch (carried into
oss as the boot-epoch-retryable change, PR #23 there), and the config seam that
`cmd/pipeline`'s own white-box test works around.

## Unrecovered numbers

`#1`, `#3`–`#11`, `#15`, `#16`, `#22` — no surviving citation. **Reserved, never
reissued.** If a reference to one of these turns up, add it above with its
provenance rather than renumbering it.

## External references not rewritten

Seven citations live outside this repository and still use bare `#N`. Rewriting
them means a PR against another repository and a sibling scope, so they were
deliberately left alone; they resolve against the table above.

| Repository | file:line | Cites | Resolves to |
| ---------- | --------- | ----- | ----------- |
| `provin.oss` | `cmd/network/main.go:173` | `#14` | `E2E-F-014` |
| `provin.oss` | `internal/netcompose/operator.go:51` | `#14` | `E2E-F-014` |
| `provin.oss` | `network/pkg/services/chainmanager/infra/nats/nats_test.go:453` | `#14` ("in the provin.e2e findings log") | `E2E-F-014` |
| `provin.oss` | `network/pkg/services/vcresolver/filestore/filestore.go:3` | `#23` | `E2E-F-023` |
| `provin.oss` | `network/pkg/services/vcresolver/filestore/backend_test.go:195` | `#23` | `E2E-F-023` |
| `provin.oss` | `network/pkg/services/auditor/filestore/filestore.go:3` | `#23` | `E2E-F-023` |
| sibling scope | `scopes/spec.provin4ai/docs/provin4ai-definition-2026-07-06.md:119` | `#13` | `E2E-F-013` |

`E2E-F-023` is the one entry with no citation inside this repository at all —
oss is its only reader, which is exactly why a repository-scoped ID matters.

**Not in that table, on purpose:**
`provin.oss` `network/pkg/services/tlogservice/mirrorstore/internal_test.go:622`
cites a `finding #2` that belongs to **oss's own code-review numbering**, not to
this register — it is about `Open` creating the mirror-store root with
`os.MkdirAll` without fsyncing the new root's parent directory. It does not
resolve to `E2E-F-002`, and reading it as though it did is the concrete harm the
namespaced IDs exist to prevent.
