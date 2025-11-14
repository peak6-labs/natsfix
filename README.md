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
    "github.com/nats-io/nats.go"
    natsfix "github.com/peak6-labs/natsfix"
    natsfixconfig "github.com/peak6-labs/natsfix/config"
    "github.com/quickfixgo/quickfix"
)

// Connect to NATS (optional - can use settings instead)
nc, err := nats.Connect("nats://localhost:4222")
if err != nil {
    log.Fatal(err)
}
defer nc.Close()

// Create FIXT.1.1 settings
settings := natsfix.CreateFIXTSettings("GATEWAY", "CLIENT1")

// Configure NATS subjects for the session
sessionID := quickfix.SessionID{
    BeginString:  "FIXT.1.1",
    SenderCompID: "GATEWAY",
    TargetCompID: "CLIENT1",
}
sessionSettings := settings.SessionSettings()[sessionID]
sessionSettings.Set(natsfixconfig.NATSInboundSubject, "fix.{BeginString}.{TargetCompID}.{SenderCompID}.msgs")
sessionSettings.Set(natsfixconfig.NATSOutboundSubject, "fix.{BeginString}.{SenderCompID}.{TargetCompID}.msgs")

// Create acceptor
acceptor, err := natsfix.NewAcceptor(
    app,                                // your quickfix.Application implementation
    storeFactory,                       // quickfix.MessageStoreFactory
    settings,
    quickfix.NewScreenLogFactory(),
    logger,                             // *slog.Logger
    natsfix.WithAcceptorConn(nc),       // optional: provide NATS connection
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
    "github.com/nats-io/nats.go"
    natsfix "github.com/peak6-labs/natsfix"
    natsfixconfig "github.com/peak6-labs/natsfix/config"
    "github.com/quickfixgo/quickfix"
)

// Connect to NATS (optional - can use settings instead)
nc, err := nats.Connect("nats://localhost:4222")
if err != nil {
    log.Fatal(err)
}
defer nc.Close()

// Create FIXT.1.1 settings
settings := natsfix.CreateFIXTSettings("CLIENT1", "GATEWAY")

// Configure NATS subjects for the session
sessionID := quickfix.SessionID{
    BeginString:  "FIXT.1.1",
    SenderCompID: "CLIENT1",
    TargetCompID: "GATEWAY",
}
sessionSettings := settings.SessionSettings()[sessionID]
sessionSettings.Set(natsfixconfig.NATSInboundSubject, "fix.{BeginString}.{TargetCompID}.{SenderCompID}.msgs")
sessionSettings.Set(natsfixconfig.NATSOutboundSubject, "fix.{BeginString}.{SenderCompID}.{TargetCompID}.msgs")

// Create initiator
initiator, err := natsfix.NewInitiator(
    app,                                // your quickfix.Application implementation
    storeFactory,                       // quickfix.MessageStoreFactory
    settings,
    quickfix.NewScreenLogFactory(),
    logger,                             // *slog.Logger
    natsfix.WithInitiatorConn(nc),      // optional: provide NATS connection
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

**NATS Connection:**

You can provide a NATS connection in two ways:

1. **Via functional option (recommended):**
```go
nc, _ := nats.Connect("nats://localhost:4222")
acceptor, err := natsfix.NewAcceptor(app, storeFactory, settings, logFactory, logger,
    natsfix.WithAcceptorConn(nc))
```

2. **Via settings (backwards compatible):**
```go
globalSettings := settings.GlobalSettings()
globalSettings.Set(natsfixconfig.NATSUrl, "nats://localhost:4222")
globalSettings.Set(natsfixconfig.NATSCredsFile, "/path/to/creds.file") // optional
acceptor, err := natsfix.NewAcceptor(app, storeFactory, settings, logFactory, logger)
// Connection will be created automatically during Start()
```

**Global Settings:**
- `NATSUrl` (required if connection not provided): NATS server URL
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
- `NewAcceptor()`: Creates acceptor, initializes sessions. Accepts optional `WithAcceptorConn()` to provide NATS connection.
- `Start()`: Connects to NATS (if not already provided), starts subscriptions and session goroutines
- `Stop()`: Unsubscribes, closes sessions, closes NATS connection
- Automatically reconnects sessions when messages are received

**Initiator:**
- `NewInitiator()`: Creates initiator, initializes sessions. Accepts optional `WithInitiatorConn()` to provide NATS connection.
- `Start()`: Connects to NATS (if not already provided), starts subscriptions and session goroutines
- `Stop()`: Cancels context, unsubscribes, closes sessions, closes NATS connection
- Automatically waits for session time before connecting
- Automatically reconnects using the configured `ReconnectInterval`
