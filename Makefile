# provin OSS — End-to-End Tests

REPOS_DIR := repos
OSS_DIR   := $(REPOS_DIR)/oss
OSS_REPO  := git@github.com:provin-line/oss.git
OSS_REF   := $(shell sed -n 's/^OSS_REF=//p' pins.env)

.PHONY: clone checkout-oss verify-pin require-oss docker-build test test-simple test-compose

## Clone repos/oss at the pinned revision (pins.env) if it is ABSENT.
##
## Does nothing when repos/oss is already there, deliberately. An existing
## checkout is the tested input: silently `git pull`ing it makes a green run
## unattributable to any revision, and rewrites a local development checkout
## out from under whoever is working in it. This target used to pull, and that
## is why a build had to be invoked by hand the one time the checkout was
## deliberately ahead of its remote.
##
## To move to a new revision: edit pins.env, then `make checkout-oss`.
clone: $(OSS_DIR)

$(OSS_DIR):
	@test -n "$(OSS_REF)" || { echo "pins.env: OSS_REF is empty or unreadable"; exit 1; }
	@mkdir -p $(REPOS_DIR)
	git clone --no-checkout $(OSS_REPO) $@
	git -C $@ checkout --detach $(OSS_REF)

## Move an existing repos/oss onto the revision pins.env names, fetching it
## first. This is how a pin bump is applied locally — explicit, never a side
## effect of building or testing.
checkout-oss:
	@test -n "$(OSS_REF)" || { echo "pins.env: OSS_REF is empty or unreadable"; exit 1; }
	@test -e $(OSS_DIR) || { echo "$(OSS_DIR) does not exist — run 'make clone' first"; exit 1; }
	git -C $(OSS_DIR) fetch origin
	git -C $(OSS_DIR) checkout --detach $(OSS_REF)

## Fail unless repos/oss is EXACTLY at the pinned revision. CI's preflight:
## without it, a checkout step that quietly resolved something else would
## still produce a green suite attributed to the pin.
##
## This fails on a symlinked repos/oss too, and that is correct rather than
## unfortunate: a symlink points at a working copy whose revision this file
## cannot speak for. Local development legitimately symlinks (see README) —
## such a checkout simply is not pin-verifiable, and a check that passed
## anyway would be worthless.
verify-pin:
	@test -n "$(OSS_REF)" || { echo "pins.env: OSS_REF is empty or unreadable"; exit 1; }
	@if [ -L $(OSS_DIR) ]; then \
		echo "$(OSS_DIR) is a symlink to $$(readlink $(OSS_DIR)) — a local working copy, whose revision pins.env cannot vouch for."; \
		echo "Pin verification applies to a real checkout only (CI always has one)."; \
		exit 1; \
	fi
	@have=$$(git -C $(OSS_DIR) rev-parse HEAD 2>/dev/null) || { echo "$(OSS_DIR) is not a git checkout"; exit 1; }; \
	if [ "$$have" != "$(OSS_REF)" ]; then \
		echo "$(OSS_DIR) is at $$have but pins.env names $(OSS_REF)."; \
		echo "Run 'make checkout-oss' to move it, or bump the pin deliberately (see pins.env)."; \
		exit 1; \
	fi; \
	echo "repos/oss is at the pinned revision $$have"

## Guard for every target that reads repos/oss. It only asserts presence —
## moving or updating the checkout is never a build step's business.
require-oss:
	@test -e $(OSS_DIR) || { \
		echo "$(OSS_DIR) is missing. Run 'make clone' to fetch the pinned revision,"; \
		echo "or symlink it at a local oss working copy (see README)."; \
		exit 1; \
	}

## Build Docker images from the oss checkout (compose runtime). A3: the compose
## topology moved off the all-in-one cmd/standalone image onto the separated
## cmd/network + cmd/pipeline pair, mirroring the process runtime's own split
## (AGENTS.md rule 3) — no scenario's docker-compose.yml references
## provin-line/standalone:local anymore.
docker-build: require-oss
	docker build -t provin-line/network:local -f $(OSS_DIR)/cmd/network/Dockerfile $(OSS_DIR)
	docker build -t provin-line/pipeline:local -f $(OSS_DIR)/cmd/pipeline/Dockerfile $(OSS_DIR)
	docker build -t provin-line/pdpstub:local -f cmd/pdpstub/Dockerfile .

## Run all scenarios AND the harness's own tests (process runtime by default;
## E2E_RUNTIME=compose for containers).
##
## ./... not ./scenarios/... : Go never runs a _test.go from an imported
## dependency, so scoping this to the scenarios would silently skip
## internal/harness's own suite — including TestComposeParity, the guard that
## enforces AGENTS.md rule 3. A guard that does not run is worse than the
## t.Skip it replaced: a skip at least prints.
test: require-oss
	go test ./... -count=1 -timeout 20m

test-simple: require-oss
	go test ./scenarios/simple/... -count=1 -timeout 10m -v

## Run the compose-runtime scenarios (requires make docker-build).
## -p 1: one compose stack at a time — five concurrent stacks flake on slower hosts.
## ./... for the same reason `test` uses it.
test-compose: require-oss
	E2E_RUNTIME=compose go test -p 1 ./... -count=1 -timeout 40m
