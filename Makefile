SHELL := /bin/sh

COMMANDS := gateway payment account recon channel-mock merchant-demo paycli

# 默认读取 deploy/.env。验证或不落盘密钥时：make up ENV_FILE=deploy/.env.example
ENV_FILE ?= deploy/.env

.PHONY: gen build lint test test-integration cover up up-lite down migrate seed e2e chaos load

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
	@test -f "$(ENV_FILE)" || { echo "缺少 $(ENV_FILE)，请复制 deploy/.env.example 为 deploy/.env"; exit 1; }
	ENV_FILE="$(ENV_FILE)" sh deploy/compose.sh --profile obs up -d
	ENV_FILE="$(ENV_FILE)" sh deploy/wait-ready.sh 180 mysql redis kafka prometheus jaeger

up-lite:
	@test -f "$(ENV_FILE)" || { echo "缺少 $(ENV_FILE)，请复制 deploy/.env.example 为 deploy/.env"; exit 1; }
	ENV_FILE="$(ENV_FILE)" sh deploy/compose.sh up -d
	ENV_FILE="$(ENV_FILE)" sh deploy/wait-ready.sh 180 mysql redis kafka

down:
	@env_file="$(ENV_FILE)"; \
	if [ ! -f "$$env_file" ]; then env_file="deploy/.env.example"; fi; \
	ENV_FILE="$$env_file" sh deploy/compose.sh --profile obs --profile apps down $(if $(filter 1,$(V)),-v,)

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
