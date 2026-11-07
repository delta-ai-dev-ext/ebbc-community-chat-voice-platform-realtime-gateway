SERVICE_NAME ?= realtime_gateway

.PHONY: install test build run lint coverage tidy fmt

# Always run first in a cold workspace: downloads every module pinned in go.sum.
install:
	go mod download

# Prescribed test entrypoint: race detector on, no cached results, bounded parallelism.
test:
	go test ./... -race -count=1 -parallel 4

# Docker image build (golang:1.26-alpine builder, distroless runtime).
build:
	docker build -t $(SERVICE_NAME):local .

run:
	go run ./cmd/server

lint:
	golangci-lint run ./...

fmt:
	gofmt -l -w .

coverage:
	go test ./... -count=1 -coverpkg=./internal/...,./cmd/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

tidy:
	go mod tidy
