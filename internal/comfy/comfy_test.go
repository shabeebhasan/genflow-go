package comfy_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shabeebhasan/genflow-go/internal/comfy"
	"github.com/shabeebhasan/genflow-go/internal/comfy/fake"
)

func TestQueueLookupDownload(t *testing.T) {
	f := fake.New(20 * time.Millisecond)
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := comfy.New(srv.URL)
	ctx := context.Background()

	name, err := c.Upload(ctx, "face.png", fake.PNG())
	if err != nil || name != "face.png" {
		t.Fatalf("upload: %q %v", name, err)
	}
	g := comfy.FaceID{Checkpoint: "sdxl.safetensors", FaceImage: name, Prompt: "studio headshot", Seed: 7, Prefix: "genflow_p1"}.Graph()
	if err := c.Queue(ctx, "p1", "w1", g); err != nil {
		t.Fatal(err)
	}
	if st, _, _, _ := c.Lookup(ctx, "p1"); st != comfy.Queued {
		t.Fatalf("want queued, got %v", st)
	}
	time.Sleep(40 * time.Millisecond)
	st, imgs, _, err := c.Lookup(ctx, "p1")
	if err != nil || st != comfy.Done || len(imgs) != 1 {
		t.Fatalf("want done with 1 image: %v %v %v", st, imgs, err)
	}
	b, err := c.Download(ctx, imgs[0])
	if err != nil || len(b) < 100 {
		t.Fatalf("download: %d bytes %v", len(b), err)
	}
	if st, _, _, _ := c.Lookup(ctx, "never-sent"); st != comfy.Unknown {
		t.Fatalf("unknown prompt reported as %v", st)
	}
}

func TestBadGraphIsPermanentAndServerErrorIsNot(t *testing.T) {
	f := fake.New(0)
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := comfy.New(srv.URL)
	err := c.Queue(context.Background(), "p2", "w", map[string]any{"1": map[string]any{}})
	if !comfy.IsPermanent(err) {
		t.Fatalf("400 should be permanent: %v", err)
	}
	f.FailNextQueue(1)
	g := comfy.FaceID{Checkpoint: "x", FaceImage: "", Prompt: "p"}.Graph()
	delete(g, "3")
	err = c.Queue(context.Background(), "p3", "w", g)
	if err == nil || comfy.IsPermanent(err) {
		t.Fatalf("500 should be retryable: %v", err)
	}
}

func TestGraphShape(t *testing.T) {
	g := comfy.FaceID{Checkpoint: "c", LoRA: "sks.safetensors", FaceImage: "f.png", Prompt: "p", Seed: 1}.Graph()
	for _, id := range []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "17"} {
		if _, ok := g[id]; !ok {
			t.Errorf("node %s missing", id)
		}
	}
	ks := g["9"].(map[string]any)["inputs"].(map[string]any)
	if ks["steps"] != 28 || ks["model"].([]any)[0] != "5" {
		t.Errorf("sampler defaults or wiring wrong: %v", ks)
	}
	plain := comfy.FaceID{Checkpoint: "c", FaceImage: "f", Prompt: "p"}.Graph()
	if _, ok := plain["2"]; ok {
		t.Error("no LoRA node expected without a LoRA")
	}
}
