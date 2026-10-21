GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty)
IMAGE ?= ghcr.io/aenix-io/twok:$(VERSION)

.PHONY: build test lint image

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
