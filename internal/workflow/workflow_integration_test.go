//go:build integration

// Real Postgres (started by the test) + fake ComfyUI.  go test -tags integration ./internal/workflow/
package workflow

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/shabeebhasan/genflow-go/internal/comfy"
	"github.com/shabeebhasan/genflow-go/internal/comfy/fake"
	"github.com/shabeebhasan/genflow-go/internal/queue"
	"github.com/shabeebhasan/genflow-go/internal/storage"
)

type env struct {
	q     *queue.Store
	fake  *fake.Server
	store storage.Local
	r     *Runner
}

var pgURL = "postgres://postgres:postgres@localhost:55434/postgres?sslmode=disable"

func TestMain(m *testing.M) {
	dir, _ := os.MkdirTemp("", "genflow-pg")
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(55434).
		RuntimePath(filepath.Join(dir, "rt")).DataPath(filepath.Join(dir, "data")).
		CachePath(filepath.Join(os.TempDir(), "supaquery-embedded-pg")).StartTimeout(60 * time.Second))
	if err := pg.Start(); err != nil {
		panic(err)
	}
	code := m.Run()
	pg.Stop()
	os.RemoveAll(dir)
	os.Exit(code)
}

func setup(t *testing.T, render time.Duration) *env {
	t.Helper()
	ctx := context.Background()
	q, err := queue.Open(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Pool.Close)
	if err := q.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	q.Pool.Exec(ctx, `TRUNCATE jobs`)
	f := fake.New(render)
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st := storage.Local{Dir: t.TempDir()}
	st.Put(ctx, "faces/me.png", fake.PNG())
	r := &Runner{Queue: q, Comfy: comfy.New(srv.URL), Store: st, Lease: 2 * time.Second,
		PollEvery: 20 * time.Millisecond, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
		IdleSleep: 10 * time.Millisecond}
	return &env{q: q, fake: f, store: st, r: r}
}

func (e *env) enqueue(t *testing.T, prompt string) string {
	id, err := e.q.Enqueue(context.Background(), Kind, Input{FaceKey: "faces/me.png",
		Params: comfy.FaceID{Checkpoint: "sdxl.safetensors", Prompt: prompt, Seed: 42}}, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// drain runs one worker until the job reaches a final state.
func (e *env) drain(t *testing.T, id string) *queue.Job {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 500; i++ {
		e.r.RunOne(ctx, "w")
		j, _ := e.q.Get(ctx, id)
		if j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job never finished")
	return nil
}

func TestHappyPath(t *testing.T) {
	e := setup(t, 30*time.Millisecond)
	id := e.enqueue(t, "studio headshot")
	j := e.drain(t, id)
	if j.State != "succeeded" || j.OutputKey == "" || j.Attempts != 1 {
		t.Fatalf("job: %+v", j)
	}
	if img, err := e.store.Get(context.Background(), j.OutputKey); err != nil || len(img) < 100 {
		t.Fatalf("output not stored: %v", err)
	}
	if e.fake.SubmitCount(id) != 1 {
		t.Fatalf("submitted %d times", e.fake.SubmitCount(id))
	}
}

// The case that matters on a GPU: the worker dies after queueing the prompt. The next worker
// must pick up the same ComfyUI run, not start a second one.
func TestCrashAfterSubmitResumesWithoutDuplicateGPURun(t *testing.T) {
	e := setup(t, 300*time.Millisecond)
	id := e.enqueue(t, "studio headshot")
	e.r.afterStep = func(step string) error {
		if step == "submit" {
			return errCrash
		}
		return nil
	}
	ctx := context.Background()
	e.r.RunOne(ctx, "worker-a") // crashes holding the lease
	j, _ := e.q.Get(ctx, id)
	if j.State != "running" || j.Step != "submit" {
		t.Fatalf("after crash: %+v", j)
	}
	e.r.afterStep = nil
	if worked, _ := e.r.RunOne(ctx, "worker-b"); worked {
		t.Fatal("worker-b took the job while worker-a's lease was still valid")
	}
	time.Sleep(e.r.Lease + 100*time.Millisecond) // lease runs out
	j = e.drain(t, id)
	if j.State != "succeeded" || j.Attempts != 2 {
		t.Fatalf("job: %+v", j)
	}
	if n := e.fake.SubmitCount(id); n != 1 {
		t.Fatalf("GPU run queued %d times, want 1", n)
	}
}

// ComfyUI restarts and forgets its queue: the job queues the same prompt_id again and finishes.
func TestComfyRestartRequeuesSamePrompt(t *testing.T) {
	e := setup(t, 200*time.Millisecond)
	id := e.enqueue(t, "studio headshot")
	go func() { time.Sleep(60 * time.Millisecond); e.fake.Restart() }()
	j := e.drain(t, id)
	if j.State != "succeeded" || e.fake.SubmitCount(id) != 2 {
		t.Fatalf("state %s, submits %d", j.State, e.fake.SubmitCount(id))
	}
}

func TestTransientErrorsRetryWithBackoff(t *testing.T) {
	e := setup(t, 10*time.Millisecond)
	e.fake.FailNextQueue(2)
	id := e.enqueue(t, "studio headshot")
	j := e.drain(t, id)
	if j.State != "succeeded" || j.Attempts != 3 {
		t.Fatalf("job: %+v", j)
	}
}

func TestExecutionErrorFailsWithoutRetry(t *testing.T) {
	e := setup(t, 10*time.Millisecond)
	e.fake.FailPrompt["cursed prompt"] = "CUDA out of memory"
	id := e.enqueue(t, "cursed prompt")
	j := e.drain(t, id)
	if j.State != "failed" || j.Attempts != 1 || j.LastError == "" {
		t.Fatalf("job: %+v", j)
	}
}

func TestRetriesRunOut(t *testing.T) {
	e := setup(t, 10*time.Millisecond)
	e.fake.FailNextQueue(100)
	id := e.enqueue(t, "studio headshot")
	j := e.drain(t, id)
	if j.State != "failed" || j.Attempts != 4 {
		t.Fatalf("job: %+v", j)
	}
}

func TestCancelStopsRunningJob(t *testing.T) {
	e := setup(t, 2*time.Second)
	e.r.Lease = 300 * time.Millisecond // heartbeat every 100ms notices the cancel quickly
	id := e.enqueue(t, "studio headshot")
	go func() { time.Sleep(150 * time.Millisecond); e.q.Cancel(context.Background(), id) }()
	e.r.RunOne(context.Background(), "w")
	j, _ := e.q.Get(context.Background(), id)
	if j.State != "cancelled" {
		t.Fatalf("job: %+v", j)
	}
}

func TestBackpressure(t *testing.T) {
	e := setup(t, 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := e.q.Enqueue(ctx, Kind, Input{}, 3, 3); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.q.Enqueue(ctx, Kind, Input{}, 3, 3); !errors.Is(err, queue.ErrBackpressure) {
		t.Fatalf("want backpressure, got %v", err)
	}
}

// Many workers, many jobs: every job runs exactly once and no prompt is queued twice.
func TestManyWorkersEachJobOnce(t *testing.T) {
	e := setup(t, 20*time.Millisecond)
	const jobs = 60
	ids := make([]string, jobs)
	for i := range ids {
		ids[i] = e.enqueue(t, "studio headshot")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		for {
			st, _ := e.q.Stats(ctx)
			if st["succeeded"] == jobs || ctx.Err() != nil {
				close(done)
				cancel()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	start := time.Now()
	e.r.Run(ctx, "pool", 8)
	<-done
	st, _ := e.q.Stats(context.Background())
	if st["succeeded"] != jobs {
		t.Fatalf("stats %v", st)
	}
	if e.fake.TotalSubmits() != jobs {
		t.Fatalf("%d submits for %d jobs", e.fake.TotalSubmits(), jobs)
	}
	t.Logf("%d jobs on 8 workers in %s", jobs, time.Since(start).Round(time.Millisecond))
}
