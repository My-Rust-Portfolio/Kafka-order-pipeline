# Kafka-order-pipeline

A minimal event-driven order pipeline in Rust using Apache Kafka. Demonstrates producing and consuming messages across multiple services with a simple domain model.

## Architecture

Topics:

- `orders.created`
- `orders.validated`
- `orders.shipped`

Services:

- **order-producer**: Generates random orders and publishes them to `orders.created`.
- **order-validator**: Consumes `orders.created`, applies a simple rule (`total_cents > 15000` → `rejected`), and publishes to `orders.validated`.
- **order-shipping**: Consumes `orders.validated`, ships only `accepted` orders, and publishes to `orders.shipped`.
- **order-metrics**: Subscribes to all three topics and prints message counts every 5 seconds.

All services run locally and connect to a single-node Kafka broker via Docker.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (with Docker Compose)
- [Rust](https://rustup.rs/) (latest stable)

## Quickstart

### 1. Start Kafka

From the project root:

```bash
docker compose up -d
```

This starts a single-node Kafka broker listening on `localhost:9092`.

### 2. Run the services

Open multiple terminals, all in the project root.

**Terminal 1 – Producer**

```bash
cargo run -p order-producer
```

**Terminal 2 – Validator**

```bash
cargo run -p order-validator
```

**Terminal 3 – Shipping**

```bash
cargo run -p order-shipping
```

**Terminal 4 – Metrics**

```bash
cargo run -p order-metrics
```

You should see:

- Orders being produced.
- Orders being validated as `accepted` or `rejected`.
- Accepted orders being shipped.
- Metrics printing counts per topic every 5 seconds.

Stop services with `Ctrl+C`. Stop Kafka with:

```bash
docker compose down
```

## Running tests

Each crate has unit tests that do not require Kafka to be running.

Run all tests:

```bash
cargo test --workspace
```

Or per crate:

```bash
cargo test -p order-producer
cargo test -p order-validator
cargo test -p order-shipping
cargo test -p order-metrics
```

## Project structure

```text
.
├── docker-compose.yml
├── Cargo.toml (workspace)
├── README.md
└── crates
    ├── order-producer
    ├── order-validator
    ├── order-shipping
    └── order-metrics
```

Each `crates/*/src/main.rs` contains a single service.

## What this demonstrates

- Using Kafka from Rust with `rdkafka`.
- Event-driven architecture with multiple topics and services.
- Basic testing of domain logic and serialization.
- Local development setup with Docker Compose.