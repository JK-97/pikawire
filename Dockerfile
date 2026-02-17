# syntax=docker/dockerfile:1.7
# Pikawire ships one binary: with the pikadump tag it embeds one sidecar
# dumper per supported pika storage-layout family (drivers/v35, v40), each
# statically linking that version's OWN storage layer. Building them runs
# PikiwiDB's dep toolchain (rocksdb/glog/...) inside this image — the
# BuildKit cache below makes repeat builds fast; first build is ~30-40 min.
#
# Build and runtime share the Rocky 8 glibc baseline (the linked archives
# are not position-independent across distros); the result runs on any
# el8-compatible base.

FROM rockylinux/rockylinux:8 AS build
RUN dnf install -y gcc gcc-c++ make cmake git python3 autoconf automake \
      libtool unzip tar gzip which findutils && dnf clean all
RUN curl -fsSL https://go.dev/dl/go1.26.5.linux-amd64.tar.gz | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:$PATH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/pikawire-pikiwidb \
    OUT=/out/pikawire bash scripts/build-cgo.sh \
 && go build -trimpath -buildvcs=false -ldflags "-s -w -X main.version=docker" -o /out/pikatool ./cmd/pikatool

FROM rockylinux/rockylinux:8-minimal
RUN mkdir -p /work && chmod 777 /work
COPY --from=build /out/pikawire /usr/local/bin/pikawire
COPY --from=build /out/pikatool /usr/local/bin/pikatool
COPY LICENSE /licenses/LICENSE
USER 65532:65532
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/pikawire"]
