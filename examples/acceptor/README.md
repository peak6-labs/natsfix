# NATS FIX Acceptor Example

A simple example demonstrating how to use the natsfix acceptor to receive FIX messages over NATS.

## Prerequisites

- Go 1.25.0+
- NATS server running (default: `localhost:4222`)

## Running

Start a NATS server:
```bash
nats-server
```

Run the acceptor:
```bash
go run main.go
```

With custom settings:
```bash
go run main.go --nats nats://localhost:4222 --sender GATEWAY --target CLIENT1 --log-level debug
```

With NATS credentials:
```bash
go run main.go --nats nats://localhost:4222 --creds /path/to/creds.file
```

## Configuration

The acceptor listens on the following NATS subjects (with defaults):
- **Inbound** (receives from client): `fix.FIXT_1_1.CLIENT1.GATEWAY`
- **Outbound** (sends to client): `fix.FIXT_1_1.GATEWAY.CLIENT1`

## Testing

Send a test message using NATS CLI:
```bash
./fixfmt.sh "8=FIXT.1.1|9=65|35=A|49=CLIENT1|56=GATEWAY|34=1|52=20250116-12:00:00|98=0|108=30|10=000|1137=9|" | nats pub "fix.FIXT_1_1.CLIENT1.GATEWAY" 
```
