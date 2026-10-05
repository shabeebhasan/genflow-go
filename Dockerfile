FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /genflow ./cmd/genflow && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fakecomfy ./cmd/fakecomfy

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /genflow /fakecomfy /
EXPOSE 8090
ENTRYPOINT ["/genflow"]
