.PHONY: build test integration lint proto demo cluster-up cluster-down failover-demo clean

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

# Starts real server processes and SIGKILLs the leader mid-stream.
integration:
	go test -tags integration -race -count=1 -v ./test/integration

lint:
	go vet ./...
	golangci-lint run ./...
	golangci-lint run --build-tags integration ./test/...
	buf lint

# Requires buf, protoc-gen-go and protoc-gen-go-grpc on PATH.
proto:
	buf generate

demo:
	./scripts/demo.sh

cluster-up:
	docker compose -f deploy/docker-compose.yml up -d --build

cluster-down:
	docker compose -f deploy/docker-compose.yml down -v

failover-demo:
	./scripts/compose-failover.sh

clean:
	rm -rf bin
