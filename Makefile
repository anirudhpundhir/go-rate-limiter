REDIS_ADDR ?= localhost:6379

.PHONY: build test test-redis bench vet fmt-check clean

build:        ## Build the server binary
	go build -o bin/ratelimiterd ./cmd/ratelimiterd

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

test:         ## Unit + miniredis tests (no external deps)
	go test -race -count=1 ./...

test-redis:   ## Full suite incl. real Redis conformance ($(REDIS_ADDR))
	RATELIMIT_TEST_REDIS_ADDR=$(REDIS_ADDR) go test -race -count=1 ./...

bench:        ## Memory + Redis benchmarks
	RATELIMIT_TEST_REDIS_ADDR=$(REDIS_ADDR) go test -run '^$$' -bench . -benchtime 2s ./limiter

clean:
	rm -rf bin
