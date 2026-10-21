GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty)
IMAGE ?= ghcr.io/aenix-io/twok:$(VERSION)
# image-etcd builds the upstream etcd image of ETCD_VERSION (without the
# leading v) with twok in the place of the etcd binary. The tag is the etcd
# version, so that a platform resolving <repository>:v<version> finds it.
ETCD_VERSION ?=
ETCD_IMAGE ?= $(shell awk '$$1 == "v$(ETCD_VERSION)" { print $$2 }' build/etcd.images)
IMAGE_ETCD ?= ghcr.io/aenix-io/twok-etcd

.PHONY: build test lint image image-etcd

build:
	go build -ldflags "-X github.com/aenix-io/twok/internal/version.Version=$(VERSION)" -o bin/twok ./cmd/twok

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run

image:
	docker buildx build --load \
		--build-arg VERSION=$(VERSION) \
		--tag $(IMAGE) \
		--file Containerfile .

image-etcd:
	@test -n "$(ETCD_VERSION)" || { echo "set ETCD_VERSION, one of: $$(cut -d' ' -f1 build/etcd.images | tr '\n' ' ')" >&2; exit 1; }
	@test -n "$(ETCD_IMAGE)" || { echo "etcd v$(ETCD_VERSION) is not pinned in build/etcd.images" >&2; exit 1; }
	docker buildx build --load \
		--build-arg VERSION=$(VERSION) \
		--build-arg ETCD_VERSION=$(ETCD_VERSION) \
		--build-arg ETCD_IMAGE=$(ETCD_IMAGE) \
		--tag $(IMAGE_ETCD):v$(ETCD_VERSION) \
		--tag $(IMAGE_ETCD):v$(ETCD_VERSION)-twok$(patsubst v%,%,$(VERSION)) \
		--file Containerfile.etcd .
