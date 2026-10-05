// Command fakecomfy serves the ComfyUI endpoints genflow uses, with no GPU, for local demos.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/shabeebhasan/genflow-go/internal/comfy/fake"
)

func main() {
	addr := flag.String("addr", ":8188", "listen address")
	render := flag.Duration("render", 3*time.Second, "how long each fake generation takes")
	flag.Parse()
	log.Printf("fake ComfyUI on %s, %s per image", *addr, *render)
	log.Fatal(http.ListenAndServe(*addr, fake.New(*render)))
}
