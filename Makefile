# Pikawire — full sync runs exclusively on the cgo dbsync engine
# (bash scripts/build-cgo.sh). Tagless builds compile (CI, tooling) but
# refuse to sync at config validation.
GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: all build build-cgo test vet fmt e2e-check clean docker

all: vet test build build-tools

# dbsync build (requires pikiwidb deps; see scripts/build-cgo.sh)
build-cgo:
	bash scripts/build-cgo.sh

# alias: the supported artifact
build: build-cgo

# protocol/debug utilities (pure Go, no pika deps)
build-tools:
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/pikatool ./cmd/pikatool

test:
	$(GO) test ./... -timeout 120s

vet:
	$(GO) vet ./...
	@$(GO) fmt ./... | grep . && { echo "run: make fmt"; exit 1; } || true

fmt:
	$(GO) fmt ./...

docker:
	docker build -t ghcr.io/jk-97/pikawire:$(VERSION) .

clean:
	rm -rf bin dist
