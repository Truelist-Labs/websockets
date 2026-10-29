# websockets

Realtime fan-out for Truelist. The Rails backend (`RealtimeService#push` in
`truelist-backend`) publishes JSON to the Redis pub/sub channel
`clearlist_realtime`; this service forwards each message to every browser socket
registered on the message's `channel` (a user's uuid). Clients: `truelist-frontend`
(`src/data/websocket.ts`) and the `truelist-marketing` free tools
(`src/lib/websocket.ts`). Production serves it at `wss://api.truelist.io/ws`.

## Layout

- `main.go`: HTTP server (`/ws`, `/up` health check), Redis subscription.
- `hub.go`: `Hub` (channel registry, `Broadcast`) and `Client` (one connection:
  read pump, write pump, ping/pong, deadlines).
- `origin.go`: browser origin allowlist.
- `*_test.go`: tests against an `httptest` server. No Redis needed: `Hub.Broadcast`
  and `Server.listen` take messages directly.

## Message protocol

Client → server (JSON text frames):

- `{"client_id":"frontend","channel":"<uuid>","request":"register_listener"}`
  subscribes the connection to `channel`. Repeats are ignored. A connection may
  register on several channels. Anything else never registers.
- `{"client_id":"frontend","channel":"<uuid>","request":"ping"}` gets
  `{"request":"pong","channel":"<uuid>"}` back on that connection.
- Anything else, including invalid JSON, is ignored.

Server → client: each Redis message re-encoded as
`{"client_id":…,"channel":…,"request":…,"data":{…}}` (e.g. `batch_updated`,
`analysis_updated`, `analysis_completed`). A Redis payload that isn't valid JSON,
or whose `data` isn't an object, is logged and skipped.

Liveness: the server sends a WS ping frame every 30s and drops a connection that
sends nothing (pong or message) for 60s. Every write has a 10s deadline. Each
connection has a 256-message send queue; a client that falls that far behind is
disconnected rather than stalling everyone else. Clients reconnect on close.

Keep the protocol wire-compatible: both frontends parse every message they get.

## Env vars

| Var | Default | |
|-----|---------|---|
| `PORT` | `8080` | |
| `REDIS_URL` | `redis://127.0.0.1:6379` | |
| `ALLOWED_ORIGINS` | `https://app.truelist.io,https://truelist.io,https://www.truelist.io` | Comma-separated; replaces the defaults. |
| `ALLOW_DEV_ORIGINS` | unset | `true` also allows `https://staging.truelist.io` and `http(s)://localhost` / `127.0.0.1` on any port. Set it on staging. |
| `DEBUG` | unset | Any value turns on verbose logging. |

Requests without an `Origin` header (non-browser clients) are allowed. The origin
check is not auth: anyone who knows a channel uuid can still subscribe to it.

## Commands

```sh
go vet ./...
go test -race ./...     # -race needs cgo (a C compiler)
go run .                # needs Redis at REDIS_URL
```

Smoke test with Redis and `wscat`:

```sh
wscat -c ws://localhost:8080/ws
> {"channel":"u1","request":"register_listener"}
> {"channel":"u1","request":"ping"}            # < {"request":"pong","channel":"u1"}
redis-cli PUBLISH clearlist_realtime '{"channel":"u1","request":"batch_updated"}'
redis-cli PUBLISH clearlist_realtime 'garbage' # logged and skipped
```

## CI and image

`.github/workflows/build-and-push.yml` runs `go vet` and `go test -race`, then
builds the Docker image. Pushes to `master` publish `ghcr.io/truelist-labs/websockets`
tagged `sha-<full sha>`, `sha-<7-char sha>`, `master` and `latest` (the
`browser_validator` convention). PRs build without pushing. Pin deployments to the
full-SHA tag. Keep the Go version in `go.mod`, the Dockerfile `GO_VERSION` default
and the workflow `build-args` in sync.

The final image is distroless (`static-debian13:nonroot`): no shell, runs as
nonroot. The health check is `GET /up`.
