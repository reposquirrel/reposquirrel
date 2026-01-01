IMAGE ?= repo-squirrel
TAG ?= latest
PORT ?= 5001
HOST ?= 0.0.0.0
READ_ONLY ?= false
REPO_DIR ?= $(PWD)/repos
STATS_DIR ?= $(PWD)/stats
CONFIG_DIR ?= $(PWD)/configuration
SAVE_FILE ?= reposquirrel.tar.gz

.PHONY: build run run-readonly save

build:
	docker build -t $(IMAGE):$(TAG) .

run:
	@mkdir -p $(REPO_DIR) $(STATS_DIR) $(CONFIG_DIR)
	docker run --rm -it \
		-p $(PORT):$(PORT) \
		-e HOST=$(HOST) \
		-e PORT=$(PORT) \
		-e READ_ONLY=$(READ_ONLY) \
		-v $(REPO_DIR):/app/repos \
		-v $(STATS_DIR):/app/stats \
		-v $(CONFIG_DIR):/app/configuration \
		$(IMAGE):$(TAG)

run-readonly:
	$(MAKE) run READ_ONLY=true

save:
	@mkdir -p $(dir $(SAVE_FILE))
	docker save $(IMAGE):$(TAG) | gzip > $(SAVE_FILE)
	@echo "Saved image to $(SAVE_FILE)"
main-to-latest:
	docker pull ghcr.io/reposquirrel/reposquirrel:main
	docker tag ghcr.io/reposquirrel/reposquirrel:main ghcr.io/reposquirrel/reposquirrel:latest
	docker push ghcr.io/reposquirrel/reposquirrel:latest
