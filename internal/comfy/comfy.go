// Package comfy is a small client for the ComfyUI HTTP API: upload an input image, queue a
// prompt graph under a prompt_id we choose, look it up in the queue or history, download outputs.
//
// Choosing the prompt_id ourselves is what makes a submit safe to retry: after a crash the
// worker asks ComfyUI about that id instead of queueing a second, duplicate GPU run.
package comfy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	Base string
	HTTP *http.Client
}

func New(base string) *Client {
	return &Client{Base: base, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

// Permanent marks an error that retrying will not fix (bad graph, missing model, 4xx).
type Permanent struct{ Err error }

func (p Permanent) Error() string { return p.Err.Error() }
func (p Permanent) Unwrap() error { return p.Err }

func IsPermanent(err error) bool {
	var p Permanent
	return errors.As(err, &p)
}

func statusErr(what string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
	err := fmt.Errorf("comfyui %s: HTTP %d: %s", what, resp.StatusCode, bytes.TrimSpace(body))
	if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
		return Permanent{err}
	}
	return err
}

// Upload puts an image into ComfyUI's input folder and returns the name a LoadImage node uses.
func (c *Client) Upload(ctx context.Context, name string, data []byte) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("image", name)
	fw.Write(data)
	mw.WriteField("overwrite", "true")
	mw.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+"/upload/image", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", statusErr("upload", resp)
	}
	var out struct {
		Name      string `json:"name"`
		Subfolder string `json:"subfolder"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Subfolder != "" {
		return out.Subfolder + "/" + out.Name, nil
	}
	return out.Name, nil
}

// Queue submits a graph under promptID. A 400 means ComfyUI rejected the graph (permanent).
func (c *Client) Queue(ctx context.Context, promptID, clientID string, graph map[string]any) error {
	buf, _ := json.Marshal(map[string]any{"prompt": graph, "prompt_id": promptID, "client_id": clientID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+"/prompt", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return statusErr("queue prompt", resp)
	}
	return nil
}

// Image is one output file reference from history.
type Image struct {
	Filename  string `json:"filename"`
	Subfolder string `json:"subfolder"`
	Type      string `json:"type"`
}

// State of a prompt as ComfyUI sees it.
type State int

const (
	Unknown State = iota // ComfyUI has never heard of it (lost in a restart, or never queued)
	Queued
	Running
	Done
	Failed
)

// Lookup reports where promptID is. For Done it returns the output images; for Failed the reason.
func (c *Client) Lookup(ctx context.Context, promptID string) (State, []Image, string, error) {
	var hist map[string]struct {
		Status struct {
			StatusStr string            `json:"status_str"`
			Completed bool              `json:"completed"`
			Messages  []json.RawMessage `json:"messages"`
		} `json:"status"`
		Outputs map[string]struct {
			Images []Image `json:"images"`
		} `json:"outputs"`
	}
	if err := c.getJSON(ctx, "/history/"+url.PathEscape(promptID), &hist); err != nil {
		return Unknown, nil, "", err
	}
	if h, ok := hist[promptID]; ok {
		if h.Status.StatusStr == "error" {
			reason := "generation failed in ComfyUI"
			for _, m := range h.Status.Messages {
				if bytes.Contains(m, []byte("execution_error")) {
					reason = string(m)
				}
			}
			return Failed, nil, reason, nil
		}
		var imgs []Image
		for _, o := range h.Outputs {
			imgs = append(imgs, o.Images...)
		}
		return Done, imgs, "", nil
	}
	var q struct {
		Running [][]any `json:"queue_running"`
		Pending [][]any `json:"queue_pending"`
	}
	if err := c.getJSON(ctx, "/queue", &q); err != nil {
		return Unknown, nil, "", err
	}
	has := func(items [][]any) bool {
		for _, it := range items {
			if len(it) > 1 && it[1] == promptID {
				return true
			}
		}
		return false
	}
	switch {
	case has(q.Running):
		return Running, nil, "", nil
	case has(q.Pending):
		return Queued, nil, "", nil
	}
	return Unknown, nil, "", nil
}

// Download fetches one output image.
func (c *Client) Download(ctx context.Context, img Image) ([]byte, error) {
	q := url.Values{"filename": {img.Filename}, "subfolder": {img.Subfolder}, "type": {img.Type}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.Base+"/view?"+q.Encode(), nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, statusErr("download", resp)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 200<<20))
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.Base+path, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return statusErr("GET "+path, resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
