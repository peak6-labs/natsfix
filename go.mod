module github.com/peak6-labs/natsfix

go 1.24

// replace github.com/quickfixgo/quickfix => ../quickfix
replace github.com/quickfixgo/quickfix => github.com/peak6-labs/quickfix v0.0.0-20251020175815-1080c6e7ae43

require (
	github.com/nats-io/nats.go v1.47.0
	github.com/quickfixgo/quickfix v0.9.10
	github.com/spf13/pflag v1.0.10
)

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pires/go-proxyproto v0.7.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/quagmt/udecimal v1.8.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/net v0.24.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
)
