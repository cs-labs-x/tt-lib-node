# tt-lib-node

Shared library for the Node services of the train ticket sales system. It
gives every service the same common base, so that the infrastructure logic
is not rewritten 20 times:

| Module (`src/`) | What it solves                                                                            |
| ---------------- | ---------------------------------------------------------------------------------------- |
| `config`         | Reads the service configuration from the environment.                                    |
| `client`         | A uniform HTTP client for the service-to-service calls.                                  |
| `events`         | Publish and consume messages through a single API, whether the transport is Kafka or RabbitMQ — the channel decides by its prefix (`kafka:...` / `rabbitmq:...`), not the service. |
| `health`         | Registers `GET /health`, common to every Node service (Fastify).                         |

Same shape as its siblings [`tt-lib-go`](https://github.com/lucas-test-repos/tt-lib-go)
and [`tt-lib-py`](https://github.com/lucas-test-repos/tt-lib-py) — a
publisher with `publish`/`close`, a consumer with `start`/`close`, and a
factory for the health handler — so that a developer who knows one of them
recognises it in the other two.

## Why it is public

The 69 Go, Node and Python services of the system —the 70 in the generated
tree minus the frontend, which consumes no library— depend on these three
libraries by their version tag (`v0.1.0`): 24 in Go, 25 in Node and 20 in
Python. Publishing them as **public** repositories is what lets the CI of
each one of those 69 services resolve the dependency without any
credential. It is the only deliberate visibility asymmetry in the whole set
of repositories.

## Usage

```ts
import { loadConfig, createClient, registerHealth } from '@lucas-test-repos/tt-lib-node'
```

## Development

```bash
npm ci
npm test
```
