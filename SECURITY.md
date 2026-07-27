# Security Policy

## Status

provin.e2e is the end-to-end test harness for the provin OSS components. It
ships no runtime artifact, operates no service, and has no users beyond the
developers and CI jobs that run it. Its own attack surface is correspondingly
narrow — so the routing below matters more here than any severity scale does.

## Where to report what

**A flaw in the system under test belongs in that system's repository, not
here.** The scenarios exercise `provin-line/oss` and the images built from
`provin-line/auth`, and reading them is a reasonable way to notice something
wrong with either. If what you found is a weakness in the node, the wire, the
credential chain, or the auth stack, report it through:

- [provin-line/oss](https://github.com/provin-line/oss/security/policy) — the
  node, the protocol surfaces, the quickstart
- [provin-line/auth](https://github.com/provin-line/auth/security/policy) — the
  DID grant, the policy verifier, the published images

**What belongs here** is a flaw in the harness itself: the CI workflows and
their handling of the GitHub App installation token, the compose fixtures and
the throwaway credentials they generate, or a supply-chain concern in the
harness's own dependencies.

There is a third category worth naming, because this repository is where it
becomes visible: **a scenario that passes when it should not**. An assertion
reporting green while the behaviour it claims to prove is absent is not a
vulnerability — but it is the exact failure this harness exists to prevent, and
a green suite is the evidence everything else leans on. Those are not
sensitive; open a normal issue. [`FINDINGS.md`](FINDINGS.md) records the ones
we have caught ourselves, including how each was proven rather than assumed.

## Reporting a vulnerability

**Please do not open a public issue for a suspected vulnerability.** Public
disclosure of an unpatched flaw puts every deployment at risk.

Report privately through **GitHub Private Vulnerability Reporting** on this
repository: the **Security** tab → **Report a vulnerability**. If the Security
tab is unavailable, email <yoshi@1o1.co.jp> instead.

Please include, to the extent you can:

- affected commit,
- impact (what an attacker gains),
- reproduction steps or a proof of concept,
- any embargo/disclosure timing you would like us to honor.

We do **not** commit to an acknowledgement or remediation SLA, and we do not
operate a bug-bounty program. We will engage on the private advisory and
coordinate disclosure with you.

## Supported versions

This repository is not versioned: it publishes no releases, tags no versions,
and ships nothing anyone installs. Only `main` is assessed, and a fix lands as
an ordinary commit.

The revision of `provin-line/oss` the scenarios are verified against is pinned
in [`pins.env`](pins.env) as a SHA rather than a branch, so any given commit of
this repository names exactly what it was green against.
