# Build stage: static binary, no cgo (distroless runtime has no libc).
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Dependencies first so the module layer is cached independently of the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/server ./cmd/server

# Runtime stage: distroless static, non-root, no shell and no package manager.
FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --from=builder /app/server ./server

USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/app/server"]
