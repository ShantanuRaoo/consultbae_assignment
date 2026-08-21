# Task 5 — Stretch: Launching to 5,000 Gig Workers Over a Weekend

## What breaks first

**Storage, almost immediately.** The current app writes uploaded audio straight to local
disk (`audio_app/uploads/`). On Railway specifically, the filesystem is ephemeral — any
redeploy, restart, or crash wipes every file that's been collected. At 5,000 submissions
this isn't a hypothetical: a single deploy during the collection window would silently
destroy a chunk of the weekend's data with no error shown to anyone.

**The matching query, right after.** `find_or_create_candidate()` runs `SELECT id, name,
phone, skills FROM candidates` — the *entire* table — on every single submission, then
scores it in Python against every row. That's fine at ~100 candidates. At 5,000+ concurrent
submitters hitting this synchronously, it becomes an O(n) full-table pull per request,
and under real concurrent load this is the first thing to visibly slow the whole app down,
well before the server itself is under any real CPU pressure.

**`ffmpeg` conversion, under concurrency.** Every submission currently spawns a blocking
`subprocess.run()` call to convert the upload to WAV before extraction. This runs
synchronously inside the request — one slow conversion blocks that worker entirely. With
many workers hitting `/submit` around the same time (a launch weekend has real bursts,
not a steady trickle), request queueing and timeouts show up here before the database does.

## What I'd change before launch

**Object storage instead of local disk.** Move uploads to S3 (or R2/Cloudflare's
equivalent) immediately — not local disk, not even a Railway Volume as a long-term
answer. A Railway Volume solves the "files disappear on redeploy" problem but doesn't
solve durability at real scale or give CDN-backed playback for the submissions list.

**Async processing instead of synchronous.** `/submit` should do the minimum possible
work inline — save the raw file, write a `pending` row — and hand off extraction +
matching to a background queue (Celery + Redis, or even a simple task queue given the
scope here). The person submitting gets an immediate "received" response instead of
waiting on ffmpeg + a full-table scan + an LLM-adjacent DB write in the same request.

**Indexed matching instead of full-table scan.** Add DB indexes on `phone` (already
present from Task 1) and consider matching via a direct SQL lookup first (`WHERE
phone = %s`) before falling back to the Python fuzzy-scoring pass — only the ambiguous
cases actually need the expensive in-memory comparison against every candidate.

## Uploads

Chunked/resumable uploads, not a single raw multipart POST. Gig workers submitting from
mobile networks will have partial uploads and dropped connections regularly at this
volume — the current app has no retry or resume logic, a failed upload just fails, full
stop, with the recording lost.

## Failures

Right now, a failed `ffmpeg` conversion or a DB error mid-request rolls back and deletes
the file — correct behavior for a single test run, but at scale this needs to be paired
with a retry queue rather than a hard failure, so a transient blip (DB connection pool
exhausted, a corrupt-but-recoverable upload) doesn't permanently lose a submission the
worker won't easily redo.

## Duplicates

The current matching logic (phone exact match, or exact/fuzzy name as weaker fallback)
was built for a ~100-row CSV dataset, not 5,000 live concurrent submitters. At this scale,
near-simultaneous submissions from the same person (e.g. someone submitting twice because
the app looked like it didn't respond) could both pass through the matching check before
either has committed, creating a duplicate candidate anyway — the current logic has no
locking or uniqueness constraint to prevent a race between two near-identical requests
processed in parallel. A unique constraint on normalized phone, or an application-level
lock during the match-and-write step, would be needed before trusting this at volume.

## Cost

Groq's free tier (used for Task 2's skill classification) caps at 8,000 tokens/minute —
already hit during small-scale testing in this project. At 5,000 submissions, if skill
classification were extended to run per-submission rather than only over the CSV-sourced
candidates, this tier would be exhausted almost immediately and would need a paid tier or
aggressive batching. Audio storage and egress (every play-button click in the submissions
view streams the file) is the other real cost driver — S3/R2 storage is cheap, but
bandwidth for thousands of recordings being played back adds up faster than storage does.