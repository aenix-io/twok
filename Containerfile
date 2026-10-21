# syntax=docker/dockerfile:1

ARG GO_IMAGE=docker.io/library/golang:1.26.10-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af
ARG BASE_IMAGE=gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags "-s -w -X github.com/aenix-io/twok/internal/version.Version=${VERSION}" \
      -o /out/twok ./cmd/twok

# This image holds twok alone, for `twok run -- etcd ...` with an etcd binary
# provided some other way, and for the twok arbiter commands. The image that
# stands in for etcd, with the real etcd inside, is built from
# Containerfile.etcd.
FROM ${BASE_IMAGE}
ARG VERSION=dev
LABEL org.opencontainers.image.title="twok" \
      org.opencontainers.image.description="twok runs etcd on two members and breaks the tie through an external arbiter" \
      org.opencontainers.image.source="https://github.com/aenix-io/twok" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=build /out/twok /usr/local/bin/twok
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/twok"]
