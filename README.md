# natsfix

A NATS-based transport layer for the FIX protocol (FIXT.1.1) using the QuickFIX/Go session engine. This library replaces the traditional TCP socket layer with NATS pub/sub messaging while maintaining full FIX protocol semantics.

## Overview

natsfix provides both `Acceptor` and `Initiator` implementations that integrate with QuickFIX/Go to handle FIX sessions over NATS. Both components manage:
- NATS connection and subscriptions per FIX session
- Message routing between NATS subjects and QuickFIX sessions
- Session lifecycle (connect, disconnect, recovery)

The **Acceptor** passively waits for incoming messages and reconnects sessions on demand. The **Initiator** actively initiates connections and automatically reconnects using the configured reconnect interval.

QuickFIX/Go handles all FIX protocol concerns (heartbeats, sequence numbers, resends, FIXT.1.1 semantics).

## Building and Testing

Build the project:
```bash
make build
# or
go build ./...
```

Run tests:
```bash
make test
# or
go test -v -race ./...
```

## Examples

See the [examples/](examples/) directory for complete working examples:
- [examples/acceptor/](examples/acceptor/) - Gateway acceptor that receives and responds to FIX orders
- [examples/initiator/](examples/initiator/) - Client initiator that sends FIX orders

## Usage

### Acceptor

The acceptor passively waits for incoming FIX messages and reconnects sessions on demand when messages arrive.

```go
import (
    natsfix "github.com/peak6-labs/natsfix"
    "github.com/quickfixgo/quickfix"
)

// Create FIXT.1.1 settings
settings := natsfix.CreateFIXTSettings("GATEWAY", "CLIENT1")

// Configure NATS connection
globalSettings := settings.GlobalSettings()
globalSettings.Set("NATSUrl", "nats://localhost:4222")
// Optional: globalSettings.Set("NATSCredsFile", "/path/to/creds.file")

// Configure NATS subjects for the session
sessionID := quickfix.SessionID{
    BeginString:  "FIXT.1.1",
    SenderCompID: "GATEWAY",
    TargetCompID: "CLIENT1",
}
sessionSettings := settings.SessionSettings()[sessionID]
sessionSettings.Set("NATSInboundSubject", "fix.{BeginString}.{TargetCompID}.{SenderCompID}.msgs")
sessionSettings.Set("NATSOutboundSubject", "fix.{BeginString}.{SenderCompID}.{TargetCompID}.msgs")

// Create acceptor
acceptor, err := natsfix.NewAcceptor(
    app,                                // your quickfix.Application implementation
    storeFactory,                       // quickfix.MessageStoreFactory
    settings,
    quickfix.NewScreenLogFactory(),
    logger,                             // *slog.Logger
)
if err != nil {
    log.Fatal(err)
}

// Start accepting connections
if err := acceptor.Start(); err != nil {
    log.Fatal(err)
}
defer acceptor.Stop()
```

### Initiator

The initiator actively connects to the counterparty, waits for session time, and automatically reconnects using the configured `ReconnectInterval`.

```go
import (
    natsfix "github.com/peak6-labs/natsfix"
    "github.com/quickfixgo/quickfix"
)

// Create FIXT.1.1 settings
settings := natsfix.CreateFIXTSettings("CLIENT1", "GATEWAY")

// Configure NATS connection
globalSettings := settings.GlobalSettings()
globalSettings.Set("NATSUrl", "nats://localhost:4222")
// Optional: globalSettings.Set("NATSCredsFile", "/path/to/creds.file")

// Configure NATS subjects for the session
sessionID := quickfix.SessionID{
    BeginString:  "FIXT.1.1",
    SenderCompID: "CLIENT1",
    TargetCompID: "GATEWAY",
}
sessionSettings := settings.SessionSettings()[sessionID]
sessionSettings.Set("NATSInboundSubject", "fix.{BeginString}.{TargetCompID}.{SenderCompID}.msgs")
sessionSettings.Set("NATSOutboundSubject", "fix.{BeginString}.{SenderCompID}.{TargetCompID}.msgs")

// Create initiator
initiator, err := natsfix.NewInitiator(
    app,                                // your quickfix.Application implementation
    storeFactory,                       // quickfix.MessageStoreFactory
    settings,
    quickfix.NewScreenLogFactory(),
    logger,                             // *slog.Logger
)
if err != nil {
    log.Fatal(err)
}

// Start initiating connections
if err := initiator.Start(); err != nil {
    log.Fatal(err)
}
defer initiator.Stop()
```

### Configuration

**Global Settings:**
- `NATSUrl` (required): NATS server URL
- `NATSCredsFile` (optional): Path to NATS credentials file

**Session Settings:**
- `NATSInboundSubject` (required): Template for receiving messages (e.g., `fix.{BeginString}.{TargetCompID}.{SenderCompID}.msgs`)
- `NATSOutboundSubject` (required): Template for sending messages (e.g., `fix.{BeginString}.{SenderCompID}.{TargetCompID}.msgs`)
- `ReconnectInterval` (initiator only): Seconds between reconnection attempts (e.g., `30`)

Subject templates support placeholders: `{BeginString}`, `{SenderCompID}`, `{TargetCompID}`, `{SenderSubID}`, `{SenderLocationID}`, `{TargetSubID}`, `{TargetLocationID}`, `{SessionQualifier}`.

### NATS Subject Pattern

For a gateway acceptor with `SenderCompID=GATEWAY` and `TargetCompID=CLIENT1`:

**Inbound** (client → gateway):
```
fix.FIXT_1_1.CLIENT1.GATEWAY.msgs
```

**Outbound** (gateway → client):
```
fix.FIXT_1_1.GATEWAY.CLIENT1.msgs
```

Subject tokens are normalized to be NATS-safe (dots, spaces, and wildcards are replaced with underscores).

### Lifecycle

**Acceptor:**
- `NewAcceptor()`: Creates acceptor, initializes sessions
- `Start()`: Connects to NATS, starts subscriptions and session goroutines
- `Stop()`: Unsubscribes, closes sessions, closes NATS connection
- Automatically reconnects sessions when messages are received

**Initiator:**
- `NewInitiator()`: Creates initiator, initializes sessions
- `Start()`: Connects to NATS, starts subscriptions and session goroutines
- `Stop()`: Cancels context, unsubscribes, closes sessions, closes NATS connection
- Automatically waits for session time before connecting
- Automatically reconnects using the configured `ReconnectInterval`
