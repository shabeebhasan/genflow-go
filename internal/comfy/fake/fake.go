// Package fake is an in-memory ComfyUI with the same HTTP endpoints, for tests and GPU-free demos.
// It can be told to fail requests or to "restart" (forget its queue), which is how the
// resume and retry paths are tested.
package fake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"sync"
	"time"
)

type prompt struct {
	graph    map[string]any
	queuedAt time.Time
}

type Server struct {
	RenderTime time.Duration // how long a prompt "runs" before it lands in history

	mu         sync.Mutex
	uploads    map[string][]byte
	queue      map[string]prompt
	history    map[string]string // prompt_id -> output filename
	failed     map[string]string
	Submits    map[string]int // how many times each prompt_id was POSTed
	failQueue  int
	failHist   int
	FailPrompt map[string]string // prompt text -> execution error to report
}

func New(render time.Duration) *Server {
	return &Server{RenderTime: render, uploads: map[string][]byte{}, queue: map[string]prompt{},
		history: map[string]string{}, failed: map[string]string{}, Submits: map[string]int{}, FailPrompt: map[string]string{}}
}

// FailNextQueue makes the next n POST /prompt calls return 500.
func (s *Server) FailNextQueue(n int) { s.mu.Lock(); s.failQueue = n; s.mu.Unlock() }

// FailNextHistory makes the next n GET /history calls return 503.
func (s *Server) FailNextHistory(n int) { s.mu.Lock(); s.failHist = n; s.mu.Unlock() }

// Restart drops everything queued or running, like a ComfyUI process restart. History stays on disk.
func (s *Server) Restart() { s.mu.Lock(); s.queue = map[string]prompt{}; s.mu.Unlock() }

func (s *Server) SubmitCount(id string) int { s.mu.Lock(); defer s.mu.Unlock(); return s.Submits[id] }

func (s *Server) TotalSubmits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, v := range s.Submits {
		n += v
	}
	return n
}

func (s *Server) settle() {
	for id, p := range s.queue {
		if time.Since(p.queuedAt) < s.RenderTime {
			continue
		}
		delete(s.queue, id)
		if reason := s.FailPrompt[promptText(p.graph)]; reason != "" {
			s.failed[id] = reason
			continue
		}
		s.history[id] = "genflow_" + id + "_00001_.png"
	}
}

func promptText(g map[string]any) string {
	if n, ok := g["6"].(map[string]any); ok {
		if in, ok := n["inputs"].(map[string]any); ok {
			s, _ := in["text"].(string)
			return s
		}
	}
	return ""
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settle()
	switch {
	case r.Method == "POST" && r.URL.Path == "/upload/image":
		f, h, err := r.FormFile("image")
		if err != nil {
			http.Error(w, "no image", 400)
			return
		}
		b, _ := io.ReadAll(f)
		s.uploads[h.Filename] = b
		json.NewEncoder(w).Encode(map[string]string{"name": h.Filename, "subfolder": "", "type": "input"})

	case r.Method == "POST" && r.URL.Path == "/prompt":
		var in struct {
			Prompt   map[string]any `json:"prompt"`
			PromptID string         `json:"prompt_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.PromptID == "" {
			http.Error(w, `{"error":"bad request"}`, 400)
			return
		}
		if s.failQueue > 0 {
			s.failQueue--
			http.Error(w, "server busy", 500)
			return
		}
		if _, ok := in.Prompt["11"]; !ok {
			http.Error(w, `{"error":{"type":"prompt_no_outputs","message":"Prompt has no outputs"}}`, 400)
			return
		}
		if n, _ := in.Prompt["3"].(map[string]any); n != nil {
			name, _ := n["inputs"].(map[string]any)["image"].(string)
			if _, ok := s.uploads[name]; !ok {
				http.Error(w, fmt.Sprintf(`{"error":"Invalid image file: %s"}`, name), 400)
				return
			}
		}
		s.Submits[in.PromptID]++
		s.queue[in.PromptID] = prompt{graph: in.Prompt, queuedAt: time.Now()}
		json.NewEncoder(w).Encode(map[string]any{"prompt_id": in.PromptID, "number": len(s.queue)})

	case r.Method == "GET" && r.URL.Path == "/queue":
		running, pending := [][]any{}, [][]any{}
		for id := range s.queue {
			pending = append(pending, []any{0, id})
		}
		json.NewEncoder(w).Encode(map[string]any{"queue_running": running, "queue_pending": pending})

	case r.Method == "GET" && len(r.URL.Path) > len("/history/") && r.URL.Path[:9] == "/history/":
		if s.failHist > 0 {
			s.failHist--
			http.Error(w, "unavailable", 503)
			return
		}
		id := r.URL.Path[9:]
		out := map[string]any{}
		if fn, ok := s.history[id]; ok {
			out[id] = map[string]any{
				"status":  map[string]any{"status_str": "success", "completed": true, "messages": []any{}},
				"outputs": map[string]any{"11": map[string]any{"images": []map[string]string{{"filename": fn, "subfolder": "", "type": "output"}}}},
			}
		} else if reason, ok := s.failed[id]; ok {
			out[id] = map[string]any{
				"status":  map[string]any{"status_str": "error", "completed": false, "messages": []any{[]any{"execution_error", map[string]string{"exception_message": reason}}}},
				"outputs": map[string]any{},
			}
		}
		json.NewEncoder(w).Encode(out)

	case r.Method == "GET" && r.URL.Path == "/view":
		w.Header().Set("Content-Type", "image/png")
		w.Write(PNG())

	default:
		http.NotFound(w, r)
	}
}

// PNG is a small valid image standing in for a generated portrait.
func PNG() []byte {
	img := image.NewRGBA(image.Rect(0, 0, 64, 96))
	for y := 0; y < 96; y++ {
		for x := 0; x < 64; x++ {
			img.Set(x, y, color.RGBA{uint8(80 + x), uint8(60 + y), 160, 255})
		}
	}
	var b bytes.Buffer
	png.Encode(&b, img)
	return b.Bytes()
}
