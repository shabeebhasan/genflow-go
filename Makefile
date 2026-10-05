.PHONY: test integration build demo
test:
	go vet ./... && go test ./...
integration:
	go test -race -tags integration -count=1 -v ./...
build:
	CGO_ENABLED=0 go build -o bin/genflow ./cmd/genflow && CGO_ENABLED=0 go build -o bin/fakecomfy ./cmd/fakecomfy
demo:
	docker compose up --build
