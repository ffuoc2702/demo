# MinIO Replication Only

This repository contains a single replication-focused solution:

MinIO1 -> Python replicator -> MinIO2

The stack does not use Kafka or Flink anymore. MinIO1 emits webhook notifications to a Python service, the service optionally scans the existing bucket on startup, deduplicates work with SQLite, and streams objects from MinIO1 to MinIO2.

## Architecture

The runtime is split into three independently started containers:

- MinIO1, exposed on `http://localhost:9000` and `http://localhost:9001`
- MinIO2, exposed on `http://localhost:9002` and `http://localhost:9003`
- Replicator, exposed on `http://localhost:8080`

Each container joins the same external Docker network: `stream-s3-replication`.

## Project Layout

Top-level files:

- `docker-compose.minio1.yml`: standalone MinIO1 stack
- `docker-compose.minio2.yml`: standalone MinIO2 stack
- `docker-compose.replicator.yml`: replicator and bootstrap stack
- `start.sh`: one-command startup for Linux/macOS shells
- `start.ps1`: one-command startup for PowerShell on Windows
- `.env.example`: environment variables used by the replicator and bootstrap script

Docker assets:

- `docker/Dockerfile.replication`: image for the Python replicator
- `docker/bootstrap-replication.sh`: bootstrap script that creates buckets, registers webhook notifications, and creates a UI user

Source code:

- `src/common`: shared helper code
	- `events.py`: event model and JSON parsing
	- `minio_io.py`: MinIO client helpers
	- `config.py`: shared config helper module kept in the repo
- `src/replication`: the active runtime
	- `app.py`: webhook server, queue, worker loop, initial sync, copy flow
	- `config.py`: replication settings loaded from environment variables
	- `dedup.py`: SQLite-backed dedup/state store

## How to Run

Recommended startup:

```bash
./start.sh
```

On Windows PowerShell:

```powershell
.\start.ps1
```

The startup scripts do two things:

1. Create the external network `stream-s3-replication` if it does not already exist.
2. Start the three compose files in order: MinIO1, MinIO2, then the replicator stack.

If you prefer manual startup, use these commands instead:

```bash
docker network create stream-s3-replication
docker compose -f docker-compose.minio1.yml up -d
docker compose -f docker-compose.minio2.yml up -d
docker compose -f docker-compose.replicator.yml up --build -d
```

## What the Bootstrap Script Does

When `docker-compose.replicator.yml` starts, it launches a `minio/mc` container that runs `docker/bootstrap-replication.sh`.

That script:

- waits until MinIO1 and MinIO2 are reachable
- creates the `images` bucket on both MinIO instances if needed
- registers a MinIO webhook notification on MinIO1
- creates a UI user for object uploads
- attaches the `readwrite` policy to that user on both MinIO instances

## Runtime Flow

The code path is centered in [src/replication/app.py](src/replication/app.py).

### 1. Service startup

`main()` reads settings from the environment, creates the shared queue, starts worker threads, and optionally launches the initial sync thread.

### 2. Initial sync

If `INITIAL_SYNC=true`, `_initial_sync()` lists objects in the source bucket and pushes synthetic events into the same queue used by webhook events.

That means initial data and realtime data follow the same downstream path.

### 3. Webhook intake

MinIO1 sends an HTTP `POST` request to `/minio-webhook` whenever an object is created.

`WebhookHandler.do_POST()` reads the body, validates the path, and enqueues the payload.

### 4. Worker execution

`_worker_loop()` continuously consumes events from the queue and calls `_replicate_event()`.

The queue is bounded, so if the service is overloaded, requests can be back-pressured instead of unboundedly consuming memory.

### 5. Copy decision

`_replicate_event()` parses the payload into a `StreamEvent` and then applies these checks:

- ignore non-copyable event types
- ignore events from buckets other than the configured source bucket
- compute a dedup key
- claim the dedup key in SQLite
- check whether the destination already has the same object

### 6. Data transfer

If the object must be copied, the replicator:

- reads metadata from MinIO1 with `stat_object`
- downloads the object body from MinIO1 with `get_object`
- uploads the object to MinIO2 with `put_object`

This is a streamed transfer. The full object is not loaded into memory at once.

### 7. Completion

After a successful transfer, the record is marked `done` in SQLite.

If any exception occurs, the record is released so a later retry can attempt the copy again.

## Dedup Strategy

Dedup is implemented in [src/replication/dedup.py](src/replication/dedup.py).

The dedup key is built from:

- bucket
- object key
- etag

That means:

- the same object version is processed once
- webhook duplicates are ignored
- an initial sync event and a realtime webhook event for the same object version collapse into one copy
- a restart does not reprocess already completed objects

### Dedup State Machine

The SQLite table stores one row per object version.

Status values:

- `processing`: the record has been claimed, but copy is not finished yet
- `done`: the object has been successfully replicated or was already up to date in MinIO2

Processing TTL:

- `DEDUP_PROCESSING_TTL_SECONDS` defaults to `600`
- if a record stays in `processing` too long, it becomes eligible again
- this protects against worker crashes or abrupt container restarts

### Claim / Complete / Release

- `claim()` inserts the dedup key with `processing`
- if the key already exists, the event is treated as duplicate and skipped
- `complete()` marks the row as `done`
- `release()` removes the row when an error happens before completion

## State Storage

Persistent state is stored in SQLite.

Default path:

- `DEDUP_DB_PATH=/data/dedup.sqlite3`

That path is mounted to the `replication-data` Docker volume in `docker-compose.replicator.yml`, so the dedup state survives restarts.

The database stores:

- dedup key
- source bucket and key
- etag
- file size
- event type
- status
- claim timestamp
- completion timestamp

## Environment Variables

Important variables in `.env.example`:

- `SOURCE_MINIO_ENDPOINT`
- `SOURCE_MINIO_ACCESS_KEY`
- `SOURCE_MINIO_SECRET_KEY`
- `SOURCE_MINIO_SECURE`
- `DEST_MINIO_ENDPOINT`
- `DEST_MINIO_ACCESS_KEY`
- `DEST_MINIO_SECRET_KEY`
- `DEST_MINIO_SECURE`
- `SOURCE_BUCKET`
- `DEST_BUCKET`
- `INITIAL_PREFIX`
- `INITIAL_SYNC`
- `REPLICATION_WEBHOOK_HOST`
- `REPLICATION_WEBHOOK_PORT`
- `REPLICATION_WORKERS`
- `REPLICATION_QUEUE_SIZE`
- `DEDUP_DB_PATH`
- `DEDUP_PROCESSING_TTL_SECONDS`
- `MINIO_UI_USER`
- `MINIO_UI_PASSWORD`

## Useful Endpoints

- MinIO1 console: `http://localhost:9001`
- MinIO2 console: `http://localhost:9003`
- Replicator webhook: `http://localhost:8080/minio-webhook`

## Design Notes

This implementation is intentionally small and practical.

Good fit:

- simple MinIO-to-MinIO sync
- moderate write load
- straightforward debug and deployment

Tradeoffs:

- single process replicator
- SQLite is local to the container volume
- no distributed queue
- no horizontal coordination across multiple replicator replicas

If you need multi-node replication workers or stronger delivery guarantees, move dedup/state to an external store such as Redis or Postgres.
