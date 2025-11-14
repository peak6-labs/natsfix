# AGENTS.md

## Build/Lint/Test Commands
- **Test**: `make test` or `go test -v -race ./...` - Run all tests with race detection
- **Test single**: `go test -v -race -run TestName ./...` - Run specific test by name
- **Build**: `make build` or `go build ./...` - Build all packages
- **Format**: `make fmt` or `go fmt ./...` - Format code
- **Vet**: `make vet` or `go vet ./...` - Run go vet static analysis
- **Lint**: `make lint` or `golangci-lint run` - Run linter
- **Tidy**: `make tidy` or `go mod tidy` - Clean up go.mod

## Architecture & Structure
This is a **NATS-based transport layer for FIX protocol (FIXT.1.1)** that integrates with QuickFIX/Go. The library replaces traditional TCP sockets with NATS pub/sub messaging while maintaining full FIX protocol semantics.

**Key components**:
- `acceptor.go`: NATSAcceptor manages NATS connections, subscriptions, and session lifecycle
- `subject.go`: NATS subject template expansion with SessionID placeholders
- `examples/`: Example usage implementations

**Dependencies**: Uses `github.com/quickfixgo/quickfix` (replaced with `../quickfix` in go.mod) and `github.com/nats-io/nats.go`.

## Code Style & Conventions
- Go 1.25.0, standard library conventions
- Use `log/slog` for structured logging with context
- Pass `context.Context` as first parameter for cancellation support
- Error handling: wrap errors with `fmt.Errorf` and `%w` for error chains
- Channels for concurrent communication (`msgIn`, `msgOut`)
- Use `sync.Once` for one-time initialization (e.g., `doConnect`)
- Defer cleanup in goroutines with panic recovery
- Prefix unexported types with lowercase, exported with uppercase
