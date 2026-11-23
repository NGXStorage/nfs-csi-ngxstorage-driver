REGISTRY_TARGET ?= quay

# NGX Storage NFS CSI driver build tooling. The former NFS-only Operator is
# archived in the Quartet repository and is not built here.

CGO_ENABLED    ?= 0
GOOS           ?= linux
GOARCH         ?= amd64

DRIVER_BINARY = cmd/csi-driver/nfs-csi-ngxstorage-driver

.PHONY: build build-driver
build: build-driver ## build the CSI driver binary

build-driver:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -o $(DRIVER_BINARY) ./cmd/csi-driver

.PHONY: test vet fmt lint
test:
	go test -count=1 ./...

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l pkg cmd 2>/dev/null)" || { echo "gofmt required:"; gofmt -l pkg cmd; exit 1; }

lint: fmt vet test ## full driver lint and test

.PHONY: docker-driver docker-all
docker-driver: ## build the commit-tagged driver image without pushing
	python3 build/image.py local

.PHONY: docker-push
docker-push: ## build and push the commit tag to Quay (clean sources required)
	python3 build/image.py "$(REGISTRY_TARGET)"

docker-all: docker-driver ## compatibility alias for the driver-only image build

.PHONY: digests ci
digests: ## print the driver image digest
	@cat build/artifacts/image.json

ci: lint build ## local driver CI gate

.PHONY: clean
clean: ## remove build artifacts
	rm -f cmd/csi-driver/nfs-csi-ngxstorage-driver
