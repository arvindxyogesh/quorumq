.PHONY: build test lint proto demo clean

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

lint:
	go vet ./...
	golangci-lint run ./...
	buf lint

# Requires buf, protoc-gen-go and protoc-gen-go-grpc on PATH.
proto:
	buf generate

demo:
	./scripts/demo.sh

clean:
	rm -rf bin
