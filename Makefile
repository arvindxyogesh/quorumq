.PHONY: build test integration lint proto demo cluster-up cluster-down observability-up failover-demo k8s-apply clean

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
	docker compose -f deploy/docker-compose.yml --profile observability down -v

# The cluster plus Prometheus (localhost:9090) and Grafana (localhost:3000).
observability-up:
	docker compose -f deploy/docker-compose.yml --profile observability up -d --build

k8s-apply:
	kubectl apply -k deploy/k8s

failover-demo:
	./scripts/compose-failover.sh

clean:
	rm -rf bin
