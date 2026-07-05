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

## Build Docker images from cloned repos (compose runtime)
docker-build: clone
	docker build -t provin-line/standalone:local -f $(REPOS_DIR)/oss/cmd/standalone/Dockerfile $(REPOS_DIR)/oss
	docker build -t provin-line/pdpstub:local -f cmd/pdpstub/Dockerfile .

## Run all scenarios (process runtime by default; E2E_RUNTIME=compose for containers)
test:
	go test ./scenarios/... -count=1 -timeout 20m

test-simple:
	go test ./scenarios/simple/... -count=1 -timeout 10m -v

## Run the compose-runtime scenarios (requires make docker-build)
test-compose:
	E2E_RUNTIME=compose go test ./scenarios/simple/... -count=1 -timeout 15m -v
