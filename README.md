# sample-im

A minimal instant-messaging service built on [pushlet](../pushlet) to show what real-time push looks like in a real (if small) application: accounts and cookie sessions, a global chat room, an online list, and one-to-one direct messages — every live update travelling through pushlet topics over Server-Sent Events, with the embedded [novaque](https://github.com/usual2970/novaque) relay on SQLite keeping publishes durable and replica-ready. One binary, two SQLite files, no Redis, no Docker.

The point of this repository is the demonstration, not the product: it exercises pushlet's SSE and WebSocket handlers, `PublishJSON` fan-out through the relay, heartbeats, slow-consumer behavior, and the shutdown ordering an embedder needs — and documents honestly where the app layer has to fill pushlet's gaps (presence) and where the demo cuts corners (see [Honest limits](#honest-limits)).

## Quickstart

```bash
# Clone the whole workspace with submodules (sample-im's go.mod replaces
# github.com/usual2970/pushlet with ../pushlet, so the pushlet checkout
# must sit next to sample-im — the workspace layout guarantees that).
git clone --recurse-submodules <this workspace's repository URL> pushlet-workspace

cd pushlet-workspace/sample-im
go run .
```

Then open <http://localhost:8080>, register an account, and you are in the room.

Requires Go 1.26.5+ (`GOTOOLCHAIN=auto`, the default, fetches the right toolchain automatically). The server listens on `:8080` and writes its SQLite files under `data/` — see [Configuration](#configuration).

## Feature → pushlet capability map

| sample-im feature | How it works | pushlet capability it exercises |
|---|---|---|
| Registration & sessions | Username/password with bcrypt, pair-uniqueness (same name + different passwords = distinct accounts), HttpOnly cookie sessions in SQLite | None — pure app layer, deliberately |
| Room chat | Browser subscribes to the fixed `room` topic; posting persists the message first, then broadcasts it synchronously | `HandleSSE` (`GET /events?topic=room`) + `PublishJSON("room", "message", …)` topic fan-out through the novaque/SQLite relay in distributed mode |
| Direct messages | Every account owns an unguessable topic `dm:<secret>` revealed only to its owner by `/api/me`; the server resolves a recipient's topic from their account record and publishes to both sides | Per-user unguessable topics + server-side `PublishJSON` (the browser never publishes) |
| Presence (online list) | The app owns it: join on every stream (re)connect, 15 s heartbeats against a 45 s TTL, a sendBeacon leave on unload, and a sweeper goroutine expiring ghosts | pushlet exports no connect/disconnect hooks — presence is the worked example of building lifecycle the app layer needs on top of pushlet |
| Transports | The browser uses `EventSource`; anything else can use WebSocket | Both `HandleSSE` (`/events`) and `HandleWebsocket` (`/ws`) are mounted and tested — see the curl/wscat examples below |
| Heartbeats | Streams stay open through idle periods | pushlet's built-in 30 s SSE comment heartbeats / WebSocket pings (sample-im keeps the default) |
| Reconnect & backfill | On reconnect the `connected` event triggers a history refetch (`GET /api/messages?after=<last seen id>`, same for the open DM conversation); messages merge keyed on server ids, never double-rendering | pushlet drops a slow consumer whose buffer fills; the client's EventSource reconnects on its own and heals — best-effort delivery made invisible |
| Graceful shutdown | Ctrl-C drains the HTTP server first (live streams finish), then stops pushlet (broker → relay consumer → novaque client), then closes the databases | The `Start`/`Stop` ordering pushlet requires of embedders |

### The raw pushlet surface, curl-able

Both transports are live even if the UI only uses SSE:

```bash
# SSE: subscribe to the room topic; watch connected, heartbeats (: comment
# lines), then every room message and presence snapshot as it happens.
curl -N 'http://localhost:8080/events?topic=room'
```

```bash
# WebSocket: receive via the initial-topic query string...
wscat -c 'ws://localhost:8080/ws?topic=room'

# ...or subscribe/unsubscribe dynamically. Commands are plain text over
# BINARY frames, so use a binary-capable client:
websocat -b ws://localhost:8080/ws
> SUB room
< OK
> UNSUB room
< OK
```

Type a message in the browser while either of these runs and watch the same event arrive outside the UI.

## Demo script

Use **three separate browsers or browser profiles — not three tabs**. Browsers cap parallel HTTP/1.1 connections per origin (six in Chrome and Firefox), the budget is shared across tabs, and every chat tab holds *two* SSE streams (room + direct messages). A third tab can starve. Separate profiles isolate both cookies and the connection budget.

1. **Register three users.** In browser A register `bob`, in B register `carol`, in C register `dave` (the `/login` page has both forms). Each lands in the chat room; all three online lists converge on `bob, carol, dave`.
2. **Room chat.** Type in any browser; the message appears in all three without a refresh — no polling, the stream delivers it. (Optional: keep `curl -N 'http://localhost:8080/events?topic=room'` running in a terminal to watch the raw SSE traffic, including the periodic heartbeat comments.)
3. **Direct messages.** In browser A, click `carol` in the online list and send a DM. Carol's browser (B) shows an unread badge on the new conversation and the message when she opens it. Dave (C) sees nothing — not in the room pane, not anywhere.
4. **Duplicate usernames.** Log out in browser C and register `bob` again with a *different* password — it works (uniqueness is per name+password pair). While both bobs are online, the online list shows `bob#xxxx` suffixes (the last four characters of each account id) so everyone can tell them apart; each reverts to plain `bob` when the other leaves.
5. **Restart persistence.** Back in the terminal: Ctrl-C, then `go run .` again. The still-open tabs reconnect on their own within seconds (the banner flashes while disconnected) — **no re-login**: sessions live in SQLite, and room history and DM history all survive the restart, backfilled by id.

## Honest limits

This is a demo, and the corners it cuts are part of the documentation:

- **DM privacy is capability-style, not authenticated channels.** A direct-message topic is an unguessable secret (`dm:` + 128-bit token), and only the owner's browser ever learns it — but anyone who does learn the string can subscribe; the channel itself checks nobody.
- **Topic strings transit query strings.** `EventSource('/events?topic=…')` puts the topic in the URL, so browser history or proxy logs could record it. (The server does its part where it can: `dm:` topics are redacted in sample-im's own logs.)
- **Presence lags hard kills.** A browser that dies without sending its leave beacon (crash, `kill -9`, network drop) stays "online" for up to ~45 s of TTL plus up to 10 s of sweep granularity.
- **Delivery is best-effort for slow consumers.** pushlet drops a connection whose send buffer fills rather than block the broker; the client reconnects and backfills by message id, so the gap heals — but there is no per-message delivery guarantee.
- **No rate limiting, no HTTPS, no email verification** — by design. Do not put this on the internet.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `SAMPLE_IM_ADDR` | `:8080` | HTTP listen address |
| `SAMPLE_IM_DATA_DIR` | `data/` | Directory for both SQLite files: `relay.db` (the novaque relay pushlet fans out through) and `app.db` (accounts, sessions, messages). Created if missing; pointing it at a fresh directory is all an isolated second instance needs |

## Deploy (Railway)

Deploy **this repo alone**. `go.mod`'s `replace` points at the sibling `../pushlet` checkout, which a standalone clone doesn't have — so the Dockerfile's first stage fetches pushlet from GitHub at a pinned ref (`PUSHLET_REF` build arg, default `main`) and lays it out at that sibling path for the build. `railway.json` in this repo points Railway at the Dockerfile.

1. Prerequisite: pushlet's GitHub `main` must include the WebSocket command-parser arity guard (the `fix/ws-command-guard` fix; released tags below it don't). Push that first, or pin another ref via a `PUSHLET_REF` service variable — Railway passes service variables to the Docker build as args.
2. In Railway: **New Project → Deploy from GitHub repo → sample-im**. No submodule toggle needed.
3. Add a **Volume** and mount it at `/data` — both SQLite files live there (`SAMPLE_IM_DATA_DIR` defaults to `/data` in the image). Without a volume, every redeploy wipes accounts, sessions, and history.
4. Networking: generate a **domain** for the service. Railway injects `PORT`; the container binds it automatically. No other variables are required.

Local image build (same Dockerfile):

```bash
# from inside sample-im/:
docker build -t sample-im .
# restricted networks: add --build-arg GOPROXY=https://goproxy.cn,direct
docker run --rm -p 8080:8080 -v "$PWD/data":/data sample-im
```

## Development

```bash
go test ./...        # whole suite
go test -race ./...  # the suite again under the race detector
go vet ./...
```

The tests are hermetic: every package opens its databases in `t.TempDir()`, SQLite is the pure-Go embedded driver (`modernc.org/sqlite`), and no test needs Docker, Redis, or a network. What lives where:

| Package | Tested by |
|---------|-----------|
| `.` (wiring) | `main_test.go`, `main_dm_test.go` — full-stack tests driving registration, SSE and WebSocket streams, and DM fan-out through the real relay round trip |
| `internal/store` | `store_test.go` — schema, pair-uniqueness races, reopen persistence |
| `internal/auth` | `auth_test.go` — validation caps, session lifecycle, page rendering |
| `internal/chat` | `chat_test.go` — posting rules, history ordering/`after` filter, publish-failure policy |
| `internal/presence` | `presence_test.go` — fake-clock TTL expiry, sweeper goroutine, duplicate names |
| `internal/dm` | `dm_test.go` — pair scoping, offline gating, topic isolation |

Input caps, for the audit-minded: username 1–32 chars of `[A-Za-z0-9_-]` after trimming; password 1–72 bytes (bcrypt's limit); room and DM bodies 1–2000 bytes after trimming; `after` must be a non-negative integer message id; `with` must be an existing user id; every endpoint that parses a JSON body caps it at 16 KiB (the presence endpoints read no body at all).

## Layout

```text
sample-im/
├── main.go                  # Wiring: env config, App (relay + pushlet + services), routes, graceful shutdown
├── main_test.go             # End-to-end: SSE, WebSocket, relay round trips, presence over HTTP
├── main_dm_test.go          # End-to-end: the DM flow through the relay
├── internal/store/          # SQLite app store: accounts, sessions, messages
├── internal/auth/           # Registration, login, sessions, the /login page
├── internal/chat/           # Room page, posting API, history endpoint
├── internal/presence/       # Online-list engine: join / heartbeat / TTL sweeper
├── internal/dm/             # Direct messages: send, caller-relative history, user lookup
└── web/                     # Embedded templates and static assets (app.js, style.css)
```

## What's next / not included

- **WebSocket client UI.** The `/ws` handler is mounted and tested; the browser UI uses SSE only. The `websocat -b` example above shows dynamic `SUB`/`UNSUB` on the same topics.
- **Two-instance cross-relay demo.** The relay *is* the shared database, so two local processes cross-deliver when they share a data directory: `SAMPLE_IM_ADDR=:8080 go run .` and `SAMPLE_IM_ADDR=:8081 SAMPLE_IM_DATA_DIR=<same dir> go run .` in another terminal — publish on one port, receive on the other (SQLite's WAL handles same-host multi-process access). A *separate* data directory gives you a fully isolated second service instead. A scripted version of this demo is not included.
- **History pagination.** History endpoints return the most recent 50 messages; there is no paging further back.
