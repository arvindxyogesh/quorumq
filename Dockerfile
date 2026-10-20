# syntax=docker/dockerfile:1

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The server, the CLI and the load generator, so the same image can run
# brokers and in-cluster clients (see test/k8s).
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bin/ ./cmd/... \
 && mkdir -p /out/data

# distroless/static: no shell or package manager, runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bin/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot
# 7000 Raft, 7070 gRPC API, 8080 admin HTTP (/healthz, /readyz, /status)
EXPOSE 7000 7070 8080
VOLUME /data
ENTRYPOINT ["/usr/local/bin/quorumq-server"]
CMD ["-addr=0.0.0.0:7070", "-raft-addr=0.0.0.0:7000", "-http-addr=0.0.0.0:8080", "-data-dir=/data"]
