# Manager

This repository currently contains the `manager` side of a cluster service.
The manager is the component that keeps cluster membership and leader state in
memory, exchanges protocol messages with peers, and drives periodic heartbeat
and election checks.

## Entry Point

`src/main.rs` selects the manager flow when the `manager` subcommand is used.
In that mode it initializes tracing, parses CLI arguments, builds the local
node identity, and starts the manager runtime.

## Responsibilities

The manager currently handles:

- node registration and disconnection
- heartbeat propagation
- cluster state exchange
- leader election requests and responses
- leader announcements
- periodic tick-based maintenance

## CLI Shape

The manager subcommand accepts the shared node settings:

- `--grpc-port`
- `--self-host`
- `--self-port` is optional and defaults to `--grpc-port`

When started as a manager, it may also connect to an existing manager using:

- `--manager-host`
- `--manager-port`

If no upstream manager is provided, the node starts its own cluster state with
epoch `0`.

## Internal Modules

- `src/manager/mod.rs` coordinates startup and shutdown
- `src/manager/service.rs` contains the cluster state machine
- `src/manager/domain.rs` defines the protocol payloads shared between nodes
- `src/manager/grpc.rs` exposes the gRPC surface used by peers
