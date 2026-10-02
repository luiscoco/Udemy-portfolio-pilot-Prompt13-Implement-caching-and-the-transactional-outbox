# PortfolioPilot — Milestone 13: Caching and the Transactional Outbox

PortfolioPilot is a teaching project: a stock portfolio manager with a live news feed and an AI
assistant, built step by step across 36 milestones. This README explains **milestone 13**: what the
coding agent was asked to build, what it actually did, what was verified, and how you can check it
yourself.

> Rules for the project live in [AGENTS.md](AGENTS.md). Current progress lives in
> [docs/project-state.md](docs/project-state.md). The detailed teaching notes for this milestone are in
> [docs/lessons/13-caching-and-transactional-outbox.md](docs/lessons/13-caching-and-transactional-outbox.md),
> and the design decision is recorded in
> [ADR 0007](docs/decisions/0007-cache-outbox-and-redis-streams.md).

---

## 1. Purpose

### What the prompt asked for

The prompt asked the coding agent to:

1. Add **Redis caching** for quote snapshots and news, with documented expiry times, random
   variation ("jitter"), versioned cache keys, and protection against many requests loading the
   same data at once ("single-flight"). Owner-specific cache entries must be separated by user and
   portfolio. **PostgreSQL stays the source of truth.**
2. Add an **`OutboxEvent`** table. Each domain change (for example, recording a trade) must save a
   typed event **in the same database transaction**.
3. Add a **dispatcher** worker that claims events safely, retries a limited number of times,
   publishes to **Redis Streams**, and records successful delivery.
4. Assume **at-least-once delivery**: duplicates can happen, so consumers must handle them. Never
   claim exactly-once.
5. Define a **bounded stream retention** policy and a **snapshot recovery** path.
6. Keep article ingestion **global**, but create **owner-only notifications**, so no user's
   financial data appears in a shared stream.
7. Test rollback, publish retry and duplicate handling. Update the architecture decision record
   (ADR) and project state.

### Key terms

| Term | Meaning |
| --- | --- |
| **Cache-aside** | The app checks the cache first. On a miss it reads the database and stores the result in the cache. |
| **TTL** | "Time to live": how long a cache entry lasts before Redis deletes it. |
| **Jitter** | A small random change to the TTL, so many entries do not all expire at the same moment. |
| **Single-flight** | When many requests miss the same cache key at once, only one loads from the database; the others wait for it. |
| **Transaction** | A group of database writes that all succeed together or all fail together. |
| **Transactional outbox** | Saving an "event" row in the same transaction as the change, then publishing it later. |
| **Redis Streams** | An append-only log in Redis. Each entry gets an ID such as `1790934404700-0`. |
| **Cursor** | A reader's position in a stream. It is *not* the event's identity. |
| **At-least-once** | Every event is delivered, but some may be delivered more than once. |
| **Idempotent consumer** | A consumer that produces the same result whether it sees an event once or twice. |
| **Lease** | A time-limited claim on a piece of work, so two workers don't process the same row. |

### Why it matters

If an application saves a trade and *then* sends a notification, a crash between the two steps
either loses the notification or announces a trade that was rolled back. The outbox pattern closes
that gap. Caching reduces repeated database reads without letting Redis become the source of truth.

### What students will learn

- How to make "save data" and "announce the change" atomic.
- Why real systems promise at-least-once delivery and how deduplication by event ID works.
- How to design cache keys that cannot leak one user's data to another.
- How streams, cursors, retention limits and snapshots fit together for reliable reconnection.
- How to test these guarantees against real PostgreSQL and Redis instead of mocks.

---

## 2. Steps performed

These steps are listed in the order they actually happened.

### Step 1 — Read the project contract and current state

The agent read `AGENTS.md`, `docs/project-state.md` and the milestone 13 entry in
`docs/project-plan.md`, then inspected the existing schema, services, worker and API code.
No partial work from an earlier attempt was found.

### Step 2 — Restore dependencies (offline)

`node_modules` was missing in this folder, so the pinned packages were restored from the local cache:

```powershell
npm.cmd ci --ignore-scripts --offline --cache .npm-cache
```

Result: 348 packages installed. **No dependency was added or upgraded.**

### Step 3 — Define the event contract

`packages/contracts/src/index.ts` gained Zod schemas for five event types:

| Event type | Audience | Published where |
| --- | --- | --- |
| `portfolio.updated` | one user | that user's stream |
| `watchlist.updated` | one user | that user's stream |
| `news.available` | one user | that user's stream |
| `quote.updated` | market (no user data) | shared market stream |
| `news.article.ingested` | system (internal) | never published; fanned out inside PostgreSQL |

Each event has a UUID `id`, `schemaVersion: 1`, a UTC `occurredAt`, an `audience`, `entityType`,
`entityId`, an optional `portfolioId`, and a strictly validated `payload`. The schema rejects, for
example, a portfolio event addressed to the shared market stream.

### Step 4 — Add the `OutboxEvent` table

- `packages/db/prisma/schema.prisma`: new `OutboxEvent` model and `OutboxStatus` enum
  (`PENDING`, `PUBLISHED`, `DEAD`).
- Migration `20261003090000_outbox`: creates the table with SQL `CHECK` rules. For example, a
  `user` event **must** have an owner and a `market` event **must not**.

### Step 5 — Write events inside existing transactions

- `portfolio-service.ts`: create, rename, archive and record trade each append an event.
  The trade event is written *before* the oversell check on purpose, so a rejected trade proves
  that the event is rolled back too.
- `watchlist-service.ts`: add, change and remove now run in transactions with events. A duplicate
  add emits nothing.
- `repositories.ts`: older mutation helpers now call the services, so no write path can skip the
  outbox.
- `ingestion.ts`: emits `quote.updated` only for newly inserted quotes, and
  `news.article.ingested` only for articles whose content actually changed.

Simplified example of the pattern:

```ts
const row = await tx.portfolioTransaction.create({ ... });
await appendEvent(tx, { type: 'portfolio.updated', /* ... */ }); // same transaction "tx"
if (!validateLongOnlyLedger(ledger)) throw new PortfolioError(409, ...); // rolls back both
```

### Step 6 — Build the outbox repository and dispatcher

- `packages/db/src/outbox.ts`:
  - **claim:** uses `FOR UPDATE SKIP LOCKED`, a 30-second lease and a fresh claim token.
  - **markPublished:** succeeds only if the claim token still matches.
  - **markFailed:** sets a backoff delay, or marks the event `DEAD` after 8 attempts.
  - **fanOutNews, purgePublished, requeueDead.**
- `apps/worker/src/outbox.ts`: `dispatchOnce` runs one claim → publish → acknowledge pass and
  removes connection URLs and passwords from stored error messages.
- `apps/worker/src/index.ts`: a new `WORKER_ROLE=outbox`.
- `packages/config/src/server.ts`: new settings `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE_MS`,
  `OUTBOX_MAX_ATTEMPTS` and `OUTBOX_POLL_MS`, with safe limits.

### Step 7 — Redis Streams, retention and recovery

`packages/db/src/event-stream.ts` and `recovery.ts` add:

- **Streams:** `pp:v1:events:user:{userId}` for each owner and `pp:v1:events:market` for market data.
- **Retention:** about 1,000 entries per user stream and 10,000 for the market stream, 30 days
  of idle expiry, and a 7-day replay window.
- **Cursors:** opaque values `v1.<epoch>.<entry-id>`. The "epoch" changes if Redis loses its data,
  so old cursors are rejected instead of silently skipping events.
- **`readEvents`:** returns either the next events or `{ status: 'reset' }`.
- **`EventDeduper` / `consumeOnce`:** apply each event UUID only once.
- **`recoverySnapshot`:** records the cursor *first*, then reads the owner's data from PostgreSQL.

### Step 8 — Cache-aside

`packages/db/src/cache.ts`, `cached-reads.ts` and `redis-keys.ts`:

| Data | Example key | TTL |
| --- | --- | --- |
| Latest quote (shared) | `pp:v1:cache:quote:latest:demo-nova` | 15 s ± 20 % (missing quote: 5 s) |
| Owner news | `pp:v1:cache:news:user:{demo-alice}:g1:portfolio:*all:limit:20` | 60 s ± 20 % |

- `v1` is the key version. Change it when the cached data shape changes.
- `g1` is the owner's *generation*. Every event for that owner increases it, which makes old
  entries unreachable.
- Single-flight works in two layers: shared promises inside one process, and a short Redis lock
  across processes.
- If Redis is missing, slow (over 250 ms) or failing, reads go straight to PostgreSQL.

### Step 9 — API routes

New authenticated routes in `apps/api/app/api/`:

- `GET /api/quotes?securityId=...` (up to 50 IDs)
- `GET /api/news` or `GET /api/news?portfolioId=...&limit=...`
- `GET /api/events/recovery`

### Step 10 — Tests, fixes and documentation

- **New tests:** unit tests in contracts, db and worker, plus a real PostgreSQL + Redis acceptance
  suite (`apps/worker/test/outbox.integration.test.ts`). The milestone 12 ingestion test was
  extended.
- **Bugs found and fixed while verifying:** see section 5.
- **Follow-up migration:** `20261003093000_outbox_db_clock` lets the database, not the app,
  timestamp when an event becomes available.
- **Docs:**
  - added ADR 0007
  - appended a dated clarification to ADR 0001 (its original decision is unchanged)
  - updated the ADR index
  - added a note to `docs/versions.md`
  - added lesson 13
  - updated `docs/project-state.md`

---

## 3. Results achieved

### Observed behavior

- **Rollback works.** An oversell attempt left no trade and no event behind.
- **Duplicates are handled.** In a simulated crash after publishing, the same event UUID appeared
  twice in the stream with two different cursors. The consumer applied it once, and the late
  acknowledgement from the "crashed" worker was rejected.
- **Retries are bounded.** A failing publish was rescheduled with backoff. After 3 attempts (the test
  setting) it became `DEAD`. Requeueing it then published it exactly once to the stream.
- **News stays private.** An article about a security Alice holds created a notification only for
  Alice. Bob's archived portfolio did not count. Redelivery created no second notification.
- **Caching works.** Twenty concurrent requests from two separate cache instances caused exactly
  **one** database load.

### Example output (from the actual smoke runs)

Outbox worker run once:

```text
PortfolioPilot worker role: outbox; data mode: mock
Outbox {"claimed":3,"published":3,"fannedOut":0,"retried":0,"dead":0,"lost":0}
```

A watchlist change by Alice, after dispatch, appeared in her Redis stream. The stream entry ID is
the cursor; `eventId` is the stable domain identity:

```text
1790934404700-0
eventId
824d2ebf-b3f7-4c8c-a371-8bdd4107c93a
type
watchlist.updated
schemaVersion
1
```

Recovery response (shortened):

```json
{"streams":{"user":"v1.a7fc4faf-6850-4426-9e11-64a35b1b0741.1790934377335-0",
            "market":"v1.a7fc4faf-6850-4426-9e11-64a35b1b0741.1790934332841-0"},
 "retention":{"replayWindowMs":604800000,"userStreamMaxEntries":1000,"marketStreamMaxEntries":10000},
 "portfolios":["demo-growth","demo-income"],"news":2}
```

### Final check results (observed)

| Check | Result |
| --- | --- |
| `npm run build` | Passed (existing Vite and Next.js warnings only) |
| `npm run typecheck` | Passed |
| `npm run check:browser-boundary` | Passed |
| Prisma schema drift check | Empty (no drift) |
| `npm run test` with all test databases + Redis | **110 passed, 0 skipped**, run twice |
| Live HTTP smoke with demo sign-in | Passed (401 signed out, 404 cross-user, 400 bad input) |

Tests per package: contracts 8, domain 8, config 7, providers 17, db 12, agent 3, web 5, api 33,
worker 17.

---

## 4. How to run and verify

### Prerequisites

- Node.js 24.21.0 and npm 11.19.0. On the original Windows machine the nvm directory had to be added
  to `PATH`:

  ```powershell
  $env:PATH='C:/Users/luisc/AppData/Local/Author Software/nvm/installs/v24.21.0;'+$env:PATH
  ```

- Docker with a PostgreSQL 17 container and a Redis 7.4 server. `compose.yaml` defines both, and
  `npm run infra:start` starts them. This milestone used an existing verification PostgreSQL
  container on port **5546** and the Compose Redis on **6379**, logical database **13**.
- Redis ≥ 7.0 and PostgreSQL ≥ 13 are required (see `docs/versions.md`).

### Install, migrate and build

```powershell
npm.cmd ci --ignore-scripts
$env:DATABASE_URL='postgresql://portfolio_local:local_only_change_me@127.0.0.1:5546/portfolio_m13_verify'
$env:REDIS_URL='redis://127.0.0.1:6379/13'
$env:DATA_MODE='mock'
npm.cmd run migrate:deploy --workspace @portfolio-pilot/db
npm.cmd run build
```

If the database does not exist yet, create it first:

```powershell
docker exec portfolio-pilot-m06-verify psql -U portfolio_local -d postgres -c "CREATE DATABASE portfolio_m13_verify"
```

### Run the outbox end to end (command line)

```powershell
$env:NODE_ENV='development'; $env:ALLOW_DEMO_SEED='true'
npm.cmd run seed:demo --workspace @portfolio-pilot/db

$env:WORKER_ROLE='ingestion'; $env:WORKER_ONCE='true'
$env:MOCK_NEWS_INTERVAL_MS='1000'; $env:INGESTION_INTERVAL_MS='1000'
npm.cmd run start --workspace @portfolio-pilot/worker

$env:WORKER_ROLE='outbox'
npm.cmd run start --workspace @portfolio-pilot/worker
```

**Expected:** the outbox worker prints a line like `Outbox {"claimed":1,"published":1,...}`.
Remove `WORKER_ONCE` to keep the workers running.

Inspect the results:

```powershell
'SELECT type, audience, status, attempts, "streamEntryId" FROM "OutboxEvent" ORDER BY sequence;' | docker exec -i portfolio-pilot-m06-verify psql -U portfolio_local -d portfolio_m13_verify
docker exec portfolio-pilot-local-redis-1 redis-cli -n 13 XRANGE pp:v1:events:market - +
```

### Try the API

Start the dev servers with the same `DATABASE_URL`/`REDIS_URL`, plus `DEMO_AUTH_ENABLED=true` and
`AUTH_BASE_URL=http://localhost:5173`:

```powershell
npm.cmd run dev
```

Open **http://localhost:5173**, sign in as Alice, and run in the browser console:

```js
await fetch('/api/news').then(r => r.json());
await fetch('/api/quotes?securityId=demo-nova').then(r => r.json());
await fetch('/api/events/recovery').then(r => r.json());
```

To see cache invalidation:

1. Add a watchlist item Alice does not already have.
2. Run the outbox worker once.
3. Check that `pp:v1:cache:gen:user:{demo-alice}` increased:

   ```powershell
   docker exec portfolio-pilot-local-redis-1 redis-cli -n 13 GET "pp:v1:cache:gen:user:{demo-alice}"
   ```

### Run the automated tests

```powershell
$env:DATA_MODE='mock'
$base='postgresql://portfolio_local:local_only_change_me@127.0.0.1:5546'
$env:AUTH_TEST_DATABASE_URL="$base/portfolio_m07_auth_verify"
$env:PORTFOLIO_TEST_DATABASE_URL="$base/portfolio_m08_verify"
$env:INGESTION_TEST_DATABASE_URL="$base/portfolio_m12_verify"
$env:OUTBOX_TEST_DATABASE_URL="$base/portfolio_m13_verify"
$env:OUTBOX_TEST_REDIS_URL='redis://127.0.0.1:6379/13'
npm.cmd run test
npm.cmd run typecheck
npm.cmd run check:browser-boundary
```

**Expected:** 110 tests pass with none skipped. The new migrations must be applied to every test
database listed above. The acceptance tests refuse non-local hosts and database names that don't
end in `_verify`. They use a random Redis key prefix and clean up after themselves. Without the
`OUTBOX_TEST_*` variables, those tests are **skipped**, not counted as passing.

---

## 5. Limitations and unfinished work

### Problems found during verification (all fixed)

1. **Wrong assumption about Redis trimming.** The first version used `max-deleted-entry-id` to
   detect trimmed streams. A live Redis check showed trimming does not update that field. A separate,
   cautious check was added. It may occasionally ask a client to reset unnecessarily, but it never
   skips an event.
2. **Clock mismatch.** Tests failed intermittently because the app's clock stamped when an event
   became available, while the database clock decided whether it could be claimed. The follow-up
   migration makes PostgreSQL set this time.
3. **Wrong test expectation.** A test expected `102.50`, but the project formats decimals as
   `102.5`.

### Not done or not verified

- **No live updates to the browser yet.** Server-sent events (SSE) and frontend streaming are
  milestones 14 and 15. In this milestone, the stream readers are used only by tests and the
  recovery route.
- **The command-line demo shows no news notifications.** The mock news feed mentions only `ACME`,
  which the seed lists on two exchanges. Ambiguous tickers are intentionally never linked
  (milestone 12 rule). The fan-out behavior is verified by the acceptance tests instead.
- **Event order is not guaranteed** for one user when several dispatchers run. Consumers should
  refetch data rather than apply changes blindly.
- **The market stream replays only about 3.5 days** at the current ingestion rate, which is shorter
  than the 7-day window. Older cursors reset to a snapshot.
- **A user's own changes reach their cached news after the dispatcher runs** (normally under a
  second), or after the 60-second TTL if the dispatcher is stopped.
- **Some cleanup is manual.** `DEAD` events need an operator to requeue them, and there is no admin
  screen or metrics export yet. Redis streams of deleted users expire only after 30 idle days.
- **Not rerun:** browser UI tests (no frontend changes) and `npm audit` (no dependency changes).
  Milestone 12's four known Prisma-tooling advisories still stand.
- **Still unverified from earlier milestones:** live Alpaca market data and Microsoft Entra sign-in.
- **Nothing was deployed or committed.** This folder is not a Git repository, and no cloud
  resources were created.
