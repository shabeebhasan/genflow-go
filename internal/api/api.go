// Package api: submit a face photo + prompt, watch the job, fetch the image, cancel.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/shabeebhasan/genflow-go/internal/comfy"
	"github.com/shabeebhasan/genflow-go/internal/queue"
	"github.com/shabeebhasan/genflow-go/internal/storage"
	"github.com/shabeebhasan/genflow-go/internal/workflow"
)

type Server struct {
	Queue       *queue.Store
	Store       storage.Store
	MaxDepth    int // queued + running jobs before new ones get 429
	MaxAttempts int
	MaxFaceMB   int64
	Checkpoint  string // default model file name in ComfyUI
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.create)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("GET /jobs/{id}/image", s.image)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// create takes multipart: face (image file), prompt, and optional seed, steps, lora, checkpoint.
func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.MaxFaceMB<<20)
	face, hdr, err := r.FormFile("face")
	if err != nil {
		fail(w, 400, "face image is required (multipart field \"face\")")
		return
	}
	defer face.Close()
	data, err := io.ReadAll(face)
	if err != nil {
		fail(w, 400, "could not read face image")
		return
	}
	if ct := http.DetectContentType(data); ct != "image/png" && ct != "image/jpeg" && ct != "image/webp" {
		fail(w, 400, "face must be a PNG, JPEG or WebP image, got "+ct)
		return
	}
	prompt := strings.TrimSpace(r.FormValue("prompt"))
	if prompt == "" {
		fail(w, 400, "prompt is required")
		return
	}
	atoi := func(k string) int { n, _ := strconv.Atoi(r.FormValue(k)); return n }
	params := comfy.FaceID{
		Checkpoint:     firstNonEmpty(r.FormValue("checkpoint"), s.Checkpoint),
		LoRA:           r.FormValue("lora"),
		Prompt:         prompt,
		NegativePrompt: r.FormValue("negative_prompt"),
		Seed:           int64(atoi("seed")),
		Steps:          atoi("steps"),
		Width:          atoi("width"),
		Height:         atoi("height"),
	}
	faceKey := "faces/" + uuid.NewString() + strings.ToLower(extOf(hdr.Filename))
	if err := s.Store.Put(r.Context(), faceKey, data); err != nil {
		fail(w, 500, err.Error())
		return
	}
	id, err := s.Queue.Enqueue(r.Context(), workflow.Kind, workflow.Input{FaceKey: faceKey, Params: params}, s.MaxAttempts, s.MaxDepth)
	if errors.Is(err, queue.ErrBackpressure) {
		w.Header().Set("Retry-After", "30")
		fail(w, 429, err.Error())
		return
	}
	if err != nil {
		fail(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]string{"id": id, "status_url": "/jobs/" + id})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	j, err := s.Queue.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, 404, "job not found")
		return
	}
	out := map[string]any{"id": j.ID, "state": j.State, "step": j.Step, "attempts": j.Attempts,
		"max_attempts": j.MaxAttempts, "last_error": j.LastError, "created_at": j.CreatedAt, "updated_at": j.UpdatedAt}
	if j.State == "succeeded" {
		out["image_url"] = "/jobs/" + j.ID + "/image"
	}
	writeJSON(w, 200, out)
}

func (s *Server) image(w http.ResponseWriter, r *http.Request) {
	j, err := s.Queue.Get(r.Context(), r.PathValue("id"))
	if err != nil || j.State != "succeeded" {
		fail(w, 404, "no image yet")
		return
	}
	b, err := s.Store.Get(r.Context(), j.OutputKey)
	if err != nil {
		fail(w, 500, "image missing from storage")
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Write(b)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	if err := s.Queue.Cancel(r.Context(), r.PathValue("id")); err != nil {
		fail(w, 409, "job is not queued or running")
		return
	}
	writeJSON(w, 200, map[string]string{"state": "cancelled"})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	st, err := s.Queue.Stats(r.Context())
	if err != nil {
		fail(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": st, "max_depth": s.MaxDepth})
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func extOf(name string) string {
	if i := strings.LastIndex(name, "."); i >= 0 && len(name)-i <= 5 {
		return name[i:]
	}
	return ".png"
}
