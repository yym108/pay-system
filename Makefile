SHELL := /bin/sh

COMMANDS := gateway payment account recon channel-mock merchant-demo paycli

.PHONY: gen build lint test test-integration cover up down migrate seed e2e chaos load

gen:
	@echo "gen 由 BASE-12 实现"
	@exit 1

build:
	@mkdir -p bin
	@for app in $(COMMANDS); do \
		go build -o "bin/$$app" "./cmd/$$app" || exit $$?; \
	done

lint:
	golangci-lint run ./...

test:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

up:
	@echo "up 由 BASE-02 实现"
	@exit 1

down:
	@echo "down 由 BASE-02 实现"
	@exit 1

migrate:
	@echo "migrate 由 BASE-03 实现"
	@exit 1

seed:
	@echo "seed 由 BASE-03 实现"
	@exit 1

e2e:
	@echo "e2e 由 QA-04 实现"
	@exit 1

chaos:
	@echo "chaos 由 QA-01 实现"
	@exit 1

load:
	@echo "load 由 QA-02 实现"
	@exit 1
