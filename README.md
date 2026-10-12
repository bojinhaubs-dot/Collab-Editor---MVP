# Minimal Go collaboration backend

One Go process serves multiple documents using Gin, gorilla/websocket, GORM and MySQL. Each document has one Room goroutine. Its bounded command queue serializes edits, snapshots, joins and permission changes. The server commits an edit and its operation log in one MySQL transaction before updating the Room snapshot or broadcasting `applied`.

Conflict handling is explicit rejection: an edit with an old `base_version` gets `VERSION_CONFLICT`, changes no content, and consumes no version. A client must sync, reconsider its edit against the new content, and submit a new request ID. The demo's position adjustment is specific to its example; this MVP does not implement OT, CRDT or automatic merging.

## Start

Requires Go 1.26 or later and MySQL 8. The default server address is `127.0.0.1:8080`. From this directory, start the supplied database with `docker compose up -d`, or use an existing MySQL database and set `MYSQL_DSN` accordingly. On PowerShell:

```powershell
$env:MYSQL_DSN='collab:collab_demo_password@tcp(127.0.0.1:3309)/collab_mvp?charset=utf8mb4&parseTime=true&loc=Local'
$env:TOKEN_ADMIN='demo-admin-token'
$env:TOKEN_ALICE='demo-alice-token'
$env:TOKEN_BOB='demo-bob-token'
$env:TOKEN_VIEWER='demo-viewer-token'
go run ./cmd/server
```

The server migrates the three tables on startup. All business routes require `Authorization: Bearer <token>`; `/healthz` is public. Token names identify users, while document roles are stored and checked by the Room. The creator is owner; only that owner can change members. The demo token named `admin` has no access to documents owned by someone else. See `.env.example` for all environment settings. Use distinct, private token values outside a local demonstration.

## Run the HTTP and WebSocket demo

In another shell, set the same `MYSQL_DSN` and four `TOKEN_*` variables, then run:

```powershell
go run ./cmd/demo
```

The client prints each HTTP/WS request, response and assertion. It creates `abc`, broadcasts `aXbc/v1`, rejects bob's stale v0 edit, syncs, explicitly adjusts this example's insertion position to 3, and reaches `aXbYc/v2`. It checks duplicate request IDs, the operation count, permission downgrades, denied old-connection writes, and full revocation. It also prints the number of send-to-`applied` latency samples and measured p50/p95; there is no unmeasured latency promise.

Copy the printed document ID. Stop and restart the server with the same database, then verify persistence with:

```powershell
go run ./cmd/demo -verify doc-... 
```

The verification mode opens a fresh WebSocket, checks its snapshot and the HTTP GET result against `aXbYc/v2`, and checks that exactly two operations remain in MySQL.

## API and limits

`POST /documents` creates a document. `GET /documents/:id` returns a full snapshot after a Room permission check. `PUT /documents/:id/members/:user_id` accepts `editor`, `viewer` or `none` from the owner. `GET /documents/:id/ws` upgrades one authenticated connection for one document. A WS client sends `edit` with `request_id`, `base_version` and `{pos,delete_count,text}`, or `sync` with `request_id`. Positions count Unicode code points, not UTF-8 bytes or UTF-16 units. Successful edits broadcast `applied` to readers including the author. A successful duplicate returns only an `ack`; it is not another edit event.

The default Room queue has 256 commands, each WS outbound queue has 64 messages, WS input is capped at 64 KiB, and document content at 1 MiB of UTF-8. These limits can be set with the four environment variables in `.env.example`. A full outbound queue disconnects the slow client, which can reconnect for a full snapshot. A full Room queue returns `SERVER_BUSY` or disconnects. One connection has one reader and one writer with deadlines and ping/pong. An owner cannot transfer or revoke ownership in this MVP. All runtime permission changes must go through this server's Room; direct SQL changes and multiple writing service processes are unsupported.

## Tests

Set `TEST_MYSQL_DSN` to a disposable MySQL database to run the real HTTP/WS/MySQL integration tests. Without it, those tests explicitly report `SKIP`; a passing command then covers only the unit tests and compilation.

```powershell
$env:TEST_MYSQL_DSN=$env:MYSQL_DSN
go test -count=1 ./...
go test -race -count=1 ./...
```

The race command needs CGO and a working C compiler on platforms where Go requires them. Tests cover the API flow, same-version conflict, exact edit/revocation queue order, transaction rollback, Unicode positions, and slow subscribers.

## Acceptance run

The four stages were run in order against a disposable MySQL 8.0.46 instance:

1. HTTP create/read at `abc/v0`, with a viewer read denied before membership was granted.
2. Real HTTP/WS integration: joining, `aXbc/v1` broadcast, stale v0 `VERSION_CONFLICT`, sync, and explicit example edit to `aXbYc/v2`.
3. Same-Room edit/revocation queue tests in both orders, old-connection write rejection after HTTP success, duplicate request handling, rollback with no broadcast, uncertain-commit recovery, Unicode splice, and slow subscriber isolation.
4. The standalone HTTP/WS demo passed all assertions. After a server restart, HTTP and fresh WS connections returned `aXbYc/v2`, bob's revoked connection could not reconnect, and MySQL still held exactly two operations. Eight send-to-`applied` samples measured p50 `5.901193ms` and p95 `6.644437ms` on this local run; these are observations, not a latency guarantee.

With `TEST_MYSQL_DSN` set, `go test -count=1 ./...` and `go test -race -count=1 ./...` both passed. The race run started from an empty test schema and exercised concurrent package migrations. On this Windows host, the race run used WSL's Go compiler and the disposable MySQL instance. The demo run used document ID `doc-a411f41dda1213de302f1f04a54b869b`.

## Larger platform direction

A larger service could shard by `document_id`, keep one write owner for each document, and fence an old owner during failover. Cross-node permission changes must reach that owner before success is returned. Pub/Sub alone does not provide the ordering or reliability needed for these guarantees. These mechanisms are outside this single-instance MVP.
