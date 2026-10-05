//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
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
	"github.com/shabeebhasan/genflow-go/internal/workflow"
)

func form(t *testing.T, face []byte, prompt string) (*bytes.Buffer, string) {
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	if face != nil {
		fw, _ := mw.CreateFormFile("face", "me.png")
		fw.Write(face)
	}
	mw.WriteField("prompt", prompt)
	mw.WriteField("seed", "7")
	mw.Close()
	return &b, mw.FormDataContentType()
}

// Whole path over HTTP: submit a face, workers drive (fake) ComfyUI, fetch the PNG.
func TestHTTPEndToEnd(t *testing.T) {
	dir := t.TempDir()
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(55435).
		RuntimePath(filepath.Join(dir, "rt")).DataPath(filepath.Join(dir, "data")).
		CachePath(filepath.Join(os.TempDir(), "supaquery-embedded-pg")).StartTimeout(60 * time.Second))
	if err := pg.Start(); err != nil {
		t.Fatal(err)
	}
	defer pg.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q, err := queue.Open(ctx, "postgres://postgres:postgres@localhost:55435/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Pool.Close()
	q.Migrate(ctx)

	comfySrv := httptest.NewServer(fake.New(50 * time.Millisecond))
	defer comfySrv.Close()
	store := storage.Local{Dir: t.TempDir()}
	go (&workflow.Runner{Queue: q, Comfy: comfy.New(comfySrv.URL), Store: store,
		PollEvery: 20 * time.Millisecond, IdleSleep: 10 * time.Millisecond}).Run(ctx, "test", 2)

	api := httptest.NewServer((&Server{Queue: q, Store: store, MaxDepth: 5, MaxAttempts: 3, MaxFaceMB: 5, Checkpoint: "sdxl.safetensors"}).Routes())
	defer api.Close()

	body, ct := form(t, fake.PNG(), "studio headshot, soft light")
	resp, err := http.Post(api.URL+"/jobs", ct, body)
	if err != nil || resp.StatusCode != 202 {
		t.Fatalf("create: %v %d", err, resp.StatusCode)
	}
	var created struct{ ID string }
	json.NewDecoder(resp.Body).Decode(&created)

	var status map[string]any
	for i := 0; i < 200; i++ {
		r, _ := http.Get(api.URL + "/jobs/" + created.ID)
		json.NewDecoder(r.Body).Decode(&status)
		r.Body.Close()
		if status["state"] == "succeeded" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status["state"] != "succeeded" {
		t.Fatalf("status %v", status)
	}
	img, _ := http.Get(api.URL + "/jobs/" + created.ID + "/image")
	png := new(bytes.Buffer)
	png.ReadFrom(img.Body)
	if img.Header.Get("Content-Type") != "image/png" || png.Len() < 100 {
		t.Fatalf("image: %s %d bytes", img.Header.Get("Content-Type"), png.Len())
	}

	// validation
	body, ct = form(t, []byte("not an image"), "x")
	if r, _ := http.Post(api.URL+"/jobs", ct, body); r.StatusCode != 400 {
		t.Fatalf("non-image accepted: %d", r.StatusCode)
	}
	body, ct = form(t, fake.PNG(), "")
	if r, _ := http.Post(api.URL+"/jobs", ct, body); r.StatusCode != 400 {
		t.Fatalf("empty prompt accepted: %d", r.StatusCode)
	}

	// backpressure over HTTP: stop the workers, fill the queue, the next request gets 429
	cancel()
	time.Sleep(50 * time.Millisecond)
	codes := []int{}
	for i := 0; i < 6; i++ {
		body, ct = form(t, fake.PNG(), "p")
		r, _ := http.Post(api.URL+"/jobs", ct, body)
		codes = append(codes, r.StatusCode)
	}
	if codes[4] != 202 || codes[5] != 429 {
		t.Fatalf("codes %v, want five 202 then 429", codes)
	}
}
