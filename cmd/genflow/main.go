// Command genflow runs the HTTP API and a pool of workers that drive ComfyUI.
//
//	genflow                 API + workers (default)
//	genflow -mode=api       API only
//	genflow -mode=worker    workers only (run one per GPU box)
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/shabeebhasan/genflow-go/internal/api"
	"github.com/shabeebhasan/genflow-go/internal/comfy"
	"github.com/shabeebhasan/genflow-go/internal/queue"
	"github.com/shabeebhasan/genflow-go/internal/storage"
	"github.com/shabeebhasan/genflow-go/internal/workflow"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil && n > 0 {
		return n
	}
	return d
}

func main() {
	mode := flag.String("mode", "all", "all | api | worker")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q, err := queue.Open(ctx, env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/genflow?sslmode=disable"))
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer q.Pool.Close()
	if err := q.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	store := storage.Local{Dir: env("DATA_DIR", "./data")}

	if *mode == "all" || *mode == "worker" {
		host, _ := os.Hostname()
		r := &workflow.Runner{Queue: q, Comfy: comfy.New(env("COMFYUI_URL", "http://127.0.0.1:8188")), Store: store, Log: log}
		n := envInt("WORKERS", 1)
		log.Info("workers", "n", n, "comfyui", env("COMFYUI_URL", "http://127.0.0.1:8188"))
		go r.Run(ctx, host, n)
	}

	if *mode == "all" || *mode == "api" {
		srv := &http.Server{
			Addr: ":" + env("PORT", "8090"),
			Handler: (&api.Server{Queue: q, Store: store, MaxDepth: envInt("MAX_QUEUE", 200),
				MaxAttempts: envInt("MAX_ATTEMPTS", 5), MaxFaceMB: 15,
				Checkpoint: env("CHECKPOINT", "sd_xl_base_1.0.safetensors")}).Routes(),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			log.Info("listening", "addr", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http", "err", err)
				stop()
			}
		}()
		defer func() {
			c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			srv.Shutdown(c)
		}()
	}
	<-ctx.Done()
	log.Info("shutting down: running jobs go back to the queue when their lease ends")
}
