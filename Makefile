.PHONY: help test build clean fmt vet lint tidy acceptor

help:
	@echo "Available targets:"
	@echo "  make test     - Run tests"
	@echo "  make build    - Build the project"
	@echo "  make acceptor - Build acceptor example to bin/"
	@echo "  make fmt      - Format code"
	@echo "  make vet      - Run go vet"
	@echo "  make lint     - Run golangci-lint"
	@echo "  make tidy     - Tidy go.mod"
	@echo "  make clean    - Clean build artifacts"

test:
	go test -v -race -coverprofile=coverage.out ./...

build:
	go build ./...

acceptor:
	mkdir -p bin
	go build -o bin/acceptor ./examples/acceptor

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	go clean
	rm -f coverage.out
	rm -rf bin/
