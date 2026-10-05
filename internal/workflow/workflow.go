// Package workflow runs a FaceID portrait generation as resumable steps:
//
//	upload  -> put the face photo into ComfyUI's input folder
//	submit  -> queue the graph under prompt_id = job id (asks ComfyUI first, never queues twice)
//	wait    -> poll until the prompt is in history, renewing the lease while the GPU works
//	download-> fetch the image and store it
//
// After each step the checkpoint is saved. A worker that crashes mid-job loses its lease;
// the next worker reads the checkpoint and continues from the step after the last saved one.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/shabeebhasan/genflow-go/internal/comfy"
	"github.com/shabeebhasan/genflow-go/internal/queue"
	"github.com/shabeebhasan/genflow-go/internal/storage"
)

const Kind = "faceid_portrait"

// Input is what a client sends: where the face photo is stored and the generation settings.
type Input struct {
	FaceKey string       `json:"face_key"`
	Params  comfy.FaceID `json:"params"`
}

// Checkpoint is saved after every step.
type Checkpoint struct {
	FaceName  string        `json:"face_name,omitempty"`
	PromptID  string        `json:"prompt_id,omitempty"`
	Requeues  int           `json:"requeues,omitempty"`
	Images    []comfy.Image `json:"images,omitempty"`
	OutputKey string        `json:"output_key,omitempty"`
}

// Queue is the part of queue.Store the runner needs.
type Queue interface {
	Claim(ctx context.Context, owner string, lease time.Duration) (*queue.Job, error)
	Heartbeat(ctx context.Context, id, owner string, lease time.Duration) error
	Checkpoint(ctx context.Context, id, owner, step string, data any) error
	Complete(ctx context.Context, id, owner, outputKey string) error
	Retry(ctx context.Context, id, owner string, cause error, delay time.Duration) (bool, error)
	Fail(ctx context.Context, id, owner string, cause error) error
}

type Runner struct {
	Queue       Queue
	Comfy       *comfy.Client
	Store       storage.Store
	Lease       time.Duration // how long a claim lasts without a heartbeat
	PollEvery   time.Duration // how often to ask ComfyUI while waiting
	MaxWait     time.Duration // give up on one attempt after this long in "wait"
	MaxRequeues int           // ComfyUI lost the prompt (it restarted): queue again at most this often
	BackoffBase time.Duration
	BackoffMax  time.Duration
	IdleSleep   time.Duration
	Log         *slog.Logger
	afterStep   func(step string) error // tests: return an error to simulate a crash right after a step is saved
}

func (r *Runner) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&r.Lease, 60*time.Second)
	set(&r.PollEvery, 2*time.Second)
	set(&r.MaxWait, 20*time.Minute)
	set(&r.BackoffBase, 5*time.Second)
	set(&r.BackoffMax, 5*time.Minute)
	set(&r.IdleSleep, time.Second)
	if r.MaxRequeues == 0 {
		r.MaxRequeues = 2
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
}

// Run starts n workers that share the queue. n is the backpressure on the GPU: at most n prompts
// from this process are in ComfyUI at once. It returns when ctx is cancelled.
func (r *Runner) Run(ctx context.Context, name string, n int) {
	r.defaults()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			for ctx.Err() == nil {
				worked, err := r.RunOne(ctx, owner)
				if err != nil {
					r.Log.Error("worker", "owner", owner, "err", err)
				}
				if !worked {
					select {
					case <-ctx.Done():
					case <-time.After(r.IdleSleep):
					}
				}
			}
		}(fmt.Sprintf("%s-%d", name, i))
	}
	wg.Wait()
}

// RunOne claims and works one job. It reports whether there was a job.
func (r *Runner) RunOne(ctx context.Context, owner string) (bool, error) {
	r.defaults()
	job, err := r.Queue.Claim(ctx, owner, r.Lease)
	if err != nil || job == nil {
		return false, err
	}
	log := r.Log.With("job", job.ID, "owner", owner, "attempt", job.Attempts, "resume_from", job.Step)
	log.Info("claimed")
	err = r.process(ctx, owner, job)
	switch {
	case err == nil:
		log.Info("succeeded")
		return true, nil
	case errors.Is(err, queue.ErrLostLease), errors.Is(err, errCrash):
		log.Warn("stopped", "reason", err) // someone else owns it now; leave it alone
		return true, nil
	case comfy.IsPermanent(err):
		log.Error("failed permanently", "err", err)
		return true, r.Queue.Fail(context.WithoutCancel(ctx), job.ID, owner, err)
	default:
		delay := r.backoff(job.Attempts)
		again, rerr := r.Queue.Retry(context.WithoutCancel(ctx), job.ID, owner, err, delay)
		log.Warn("attempt failed", "err", err, "retry", again, "in", delay)
		return true, rerr
	}
}

// backoff doubles per attempt with jitter, so many failed jobs do not hit ComfyUI at the same moment.
func (r *Runner) backoff(attempt int) time.Duration {
	d := r.BackoffBase << min(attempt-1, 10)
	if d > r.BackoffMax || d <= 0 {
		d = r.BackoffMax
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

var errCrash = errors.New("simulated crash")

var order = []string{"", "upload", "submit", "wait", "download"}

func next(step string) string {
	for i, s := range order {
		if s == step && i+1 < len(order) {
			return order[i+1]
		}
	}
	return ""
}

func (r *Runner) process(ctx context.Context, owner string, job *queue.Job) error {
	var in Input
	if err := json.Unmarshal(job.Input, &in); err != nil {
		return comfy.Permanent{Err: fmt.Errorf("bad job input: %w", err)}
	}
	var cp Checkpoint
	json.Unmarshal(job.Checkpoint, &cp)
	save := func(step string) error {
		if err := r.Queue.Checkpoint(ctx, job.ID, owner, step, cp); err != nil {
			return err
		}
		if r.afterStep != nil {
			return r.afterStep(step)
		}
		return nil
	}

	for step := next(job.Step); step != ""; step = next(step) {
		switch step {
		case "upload":
			face, err := r.Store.Get(ctx, in.FaceKey)
			if err != nil {
				return comfy.Permanent{Err: fmt.Errorf("face photo missing: %w", err)}
			}
			if cp.FaceName, err = r.Comfy.Upload(ctx, "genflow-"+job.ID+".png", face); err != nil {
				return err
			}
		case "submit":
			if err := r.submit(ctx, job, in, &cp); err != nil {
				return err
			}
		case "wait":
			if err := r.wait(ctx, owner, job, in, &cp); err != nil {
				return err
			}
		case "download":
			if len(cp.Images) == 0 {
				return comfy.Permanent{Err: errors.New("ComfyUI finished without an image output")}
			}
			img, err := r.Comfy.Download(ctx, cp.Images[0])
			if err != nil {
				return err
			}
			cp.OutputKey = "outputs/" + job.ID + ".png"
			if err := r.Store.Put(ctx, cp.OutputKey, img); err != nil {
				return err
			}
		}
		if err := save(step); err != nil {
			return err
		}
	}
	return r.Queue.Complete(ctx, job.ID, owner, cp.OutputKey)
}

// submit is idempotent: the prompt_id is the job id, and ComfyUI is asked before queueing.
func (r *Runner) submit(ctx context.Context, job *queue.Job, in Input, cp *Checkpoint) error {
	cp.PromptID = job.ID
	st, _, _, err := r.Comfy.Lookup(ctx, cp.PromptID)
	if err != nil {
		return err
	}
	if st != comfy.Unknown {
		return nil // already queued, running or done from an earlier attempt: do not run it twice
	}
	p := in.Params
	p.FaceImage = cp.FaceName
	p.Prefix = "genflow_" + job.ID
	return r.Comfy.Queue(ctx, cp.PromptID, "genflow", p.Graph())
}

func (r *Runner) wait(ctx context.Context, owner string, job *queue.Job, in Input, cp *Checkpoint) error {
	deadline := time.Now().Add(r.MaxWait)
	lastBeat := time.Now()
	for {
		st, imgs, reason, err := r.Comfy.Lookup(ctx, cp.PromptID)
		switch {
		case err != nil:
			return err // retryable: ComfyUI unreachable for now
		case st == comfy.Done:
			cp.Images = imgs
			return nil
		case st == comfy.Failed:
			return comfy.Permanent{Err: fmt.Errorf("ComfyUI execution error: %.300s", reason)}
		case st == comfy.Unknown:
			// ComfyUI restarted and dropped its queue. Queue the same prompt_id again, a bounded number of times.
			if cp.Requeues >= r.MaxRequeues {
				return fmt.Errorf("ComfyUI lost prompt %s %d times", cp.PromptID, cp.Requeues)
			}
			cp.Requeues++
			if err := r.submit(ctx, job, in, cp); err != nil {
				return err
			}
			if err := r.Queue.Checkpoint(ctx, job.ID, owner, "submit", cp); err != nil {
				return err
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("prompt %s still not finished after %s", cp.PromptID, r.MaxWait)
		}
		if time.Since(lastBeat) > r.Lease/3 {
			if err := r.Queue.Heartbeat(ctx, job.ID, owner, r.Lease); err != nil {
				return err // cancelled or taken over
			}
			lastBeat = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.PollEvery):
		}
	}
}
