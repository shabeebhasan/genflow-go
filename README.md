# genflow-go

A durable job runner in Go for long ComfyUI generations. Send a face photo and a prompt; workers
drive a ComfyUI FaceID graph (IPAdapter FaceID, optional LoRA, KSampler) and store the portrait.
Jobs survive worker crashes and ComfyUI restarts, retry with backoff, and never put the same
generation on the GPU twice.

It is a Go take on two pieces of my FaceSearch SaaS work: the Celery task that submits an SDXL
FaceID generation to an AI engine and polls until it finishes, and the Python script that builds
the ComfyUI FaceID graph and waits on `/history`.

## Why it exists

A GPU generation takes minutes. In the Celery version the whole generation lives inside one task
function: submit, then poll. If the worker dies while waiting, the progress and the link to the
running generation are lost, and any new attempt starts over with a fresh submit, a second run of
the same image on the GPU. genflow fixes that with three ideas:

1. **Steps with checkpoints.** `upload -> submit -> wait -> download`. Progress is saved in
   Postgres after each step, and the next attempt starts after the last saved step.
2. **Leases.** A worker claims a job with `FOR UPDATE SKIP LOCKED` and renews a lease while
   ComfyUI works. If the worker dies, the lease runs out and another worker takes over. Every
   write checks the lease owner, so a frozen worker that wakes up cannot overwrite the new owner.
3. **An idempotent submit.** The ComfyUI `prompt_id` is the job id. Before queueing, the worker
   asks ComfyUI (`/history`, `/queue`) whether that id is already queued, running or done. Only an
   id ComfyUI has never seen gets queued, so a resumed job re-attaches to the GPU run already in
   flight. If ComfyUI itself restarted and lost the prompt, the same id is queued again, a bounded
   number of times.

## Also

- **Retries with backoff:** exponential with jitter, capped; attempts are limited per job.
- **Permanent vs. temporary errors:** a rejected graph, a missing model or a ComfyUI execution
  error (for example CUDA out of memory) fails at once; a 5xx or an unreachable ComfyUI retries.
- **Backpressure:** `MAX_QUEUE` limits queued + running jobs, beyond which the API answers
  `429` with `Retry-After`. `WORKERS` limits how many prompts one process has on the GPU.
- **Cancel:** a running worker notices at its next heartbeat and stops.
- **Graceful shutdown:** running jobs return to the queue when their lease ends.

## Tested

Integration tests run against a real Postgres (started by the test) and a fake ComfyUI that speaks
the same API and can fail on purpose:

| Test | What it proves |
|---|---|
| Crash after submit | worker A dies after queueing; worker B cannot take the job while A's lease is valid, then resumes it; **the prompt reached ComfyUI exactly once** |
| ComfyUI restart | ComfyUI drops its queue mid-run; the job queues the same prompt_id again and finishes |
| Transient errors | two 500s from `/prompt`, then success on attempt 3 |
| Execution error | CUDA out of memory fails the job on attempt 1, no retries |
| Retries run out | the job stops at `max_attempts` |
| Cancel | a running job stops at the next heartbeat |
| Backpressure | the queue refuses work past its limit; over HTTP the 6th request gets 429 |
| 60 jobs, 8 workers | every job runs once, 60 submits for 60 jobs |
| HTTP end to end | upload a face, poll the job, download the PNG |

All of it also passes under `go test -race`, three runs in a row.

Not tested here: a real GPU run. The ComfyUI graph mirrors the FaceID graph my Python script
builds for ComfyUI, but this repository's tests use the fake server.

## API

| Method | Path | |
|---|---|---|
| POST | `/jobs` | multipart: `face` (image), `prompt`, optional `seed`, `steps`, `width`, `height`, `lora`, `checkpoint`, `negative_prompt` -> `202 {"id"}` or `429` |
| GET | `/jobs/{id}` | state, step, attempts, last error, `image_url` when done |
| GET | `/jobs/{id}/image` | the PNG |
| POST | `/jobs/{id}/cancel` | |
| GET | `/metrics` | jobs per state |

## Run

```bash
make demo            # docker compose: Postgres + fake ComfyUI + genflow on :8090
curl -F face=@me.png -F prompt="studio headshot, soft light" localhost:8090/jobs
curl localhost:8090/jobs/<id>
```

With a real ComfyUI (GPU box, IPAdapter Plus nodes installed):

```bash
DATABASE_URL=postgres://... COMFYUI_URL=http://gpu-box:8188 WORKERS=1 \
CHECKPOINT=sd_xl_base_1.0.safetensors ./bin/genflow
```

`-mode=api` and `-mode=worker` split the API and the workers, so one worker process can run next
to each GPU while the API runs elsewhere.

## Tests

```bash
make test         # unit tests
make integration  # Postgres + fake ComfyUI, with -race
```

## Layout

```
cmd/genflow        API + worker pool
cmd/fakecomfy      GPU-free ComfyUI stand-in for demos
internal/comfy     ComfyUI client (upload, queue with our prompt_id, queue/history lookup, view) + FaceID graph
internal/queue     Postgres queue: SKIP LOCKED claims, leases, fenced checkpoints, retry/backoff, backpressure
internal/workflow  the resumable steps
internal/storage   local storage (atomic writes); S3 fits behind the same interface
internal/api       HTTP API
```
