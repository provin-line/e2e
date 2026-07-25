# provin OSS — End-to-End Tests

REPOS_DIR := repos

# name|url (pipe-separated to avoid conflict with git SSH colon)
REPOS := \
	oss|git@github.com:provin-line/oss.git

.PHONY: clone docker-build test test-simple test-compose

## Clone or update dependency repositories
clone:
	@mkdir -p $(REPOS_DIR)
	@$(foreach repo,$(REPOS),\
		$(eval name := $(word 1,$(subst |, ,$(repo))))\
		$(eval url := $(word 2,$(subst |, ,$(repo))))\
		if [ -d "$(REPOS_DIR)/$(name)" ]; then \
			echo "Updating $(REPOS_DIR)/$(name)"; \
			git -C $(REPOS_DIR)/$(name) pull || echo "  -> $(name) pull failed, continuing"; \
		else \
			echo "Cloning $(url) -> $(REPOS_DIR)/$(name)"; \
			git clone $(url) $(REPOS_DIR)/$(name) || echo "  -> $(name) clone failed, continuing"; \
		fi;)

## Build Docker images from cloned repos (compose runtime). A3: the compose
## topology moved off the all-in-one cmd/standalone image onto the separated
## cmd/network + cmd/pipeline pair, mirroring the process runtime's own split
## (AGENTS.md rule 3) — no scenario's docker-compose.yml references
## provin-line/standalone:local anymore.
docker-build: clone
	docker build -t provin-line/network:local -f $(REPOS_DIR)/oss/cmd/network/Dockerfile $(REPOS_DIR)/oss
	docker build -t provin-line/pipeline:local -f $(REPOS_DIR)/oss/cmd/pipeline/Dockerfile $(REPOS_DIR)/oss
	docker build -t provin-line/pdpstub:local -f cmd/pdpstub/Dockerfile .

## Run all scenarios AND the harness's own tests (process runtime by default;
## E2E_RUNTIME=compose for containers).
##
## ./... not ./scenarios/... : Go never runs a _test.go from an imported
## dependency, so scoping this to the scenarios would silently skip
## internal/harness's own suite — including TestComposeParity, the guard that
## enforces AGENTS.md rule 3. A guard that does not run is worse than the
## t.Skip it replaced: a skip at least prints.
test:
	go test ./... -count=1 -timeout 20m

test-simple:
	go test ./scenarios/simple/... -count=1 -timeout 10m -v

## Run the compose-runtime scenarios (requires make docker-build).
## -p 1: one compose stack at a time — five concurrent stacks flake on slower hosts.
## ./... for the same reason `test` uses it.
test-compose:
	E2E_RUNTIME=compose go test -p 1 ./... -count=1 -timeout 40m
