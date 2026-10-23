SHELL := /bin/sh

COMMANDS := gateway payment account recon channel-mock merchant-demo paycli

# 默认读取 deploy/.env。验证或不落盘密钥时：make up ENV_FILE=deploy/.env.example
ENV_FILE ?= deploy/.env

.PHONY: gen gen-check build lint test test-integration cover up up-lite down migrate seed seed-load e2e chaos load

# 生成器钉死 v0.16.3。不传 -service，因此不生成 main 与 handler。
gen:
	@for idl in idl/account.thrift idl/payment.thrift idl/recon.thrift; do \
		go run github.com/cloudwego/kitex/tool/cmd/kitex@v0.16.3 -module github.com/yym108/pay-system -gen-path kitex_gen "$$idl" || exit $$?; \
	done

gen-check: gen
	git diff --exit-code kitex_gen

build:
	@mkdir -p bin
	@for app in $(COMMANDS); do \
		go build -o "bin/$$app" "./cmd/$$app" || exit $$?; \
	done

# 带 integration 标签：未打标签的文件照常检查，集成测试文件也一起检查。
lint:
	golangci-lint run --build-tags integration ./...

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

# DIR=down 回滚后可再 make migrate。不启动应用容器。
migrate:
	@test -f "$(ENV_FILE)" || { echo "缺少 $(ENV_FILE)，请复制 deploy/.env.example 为 deploy/.env"; exit 1; }
	ENV_FILE="$(ENV_FILE)" sh deploy/migrate.sh $(if $(DIR),$(DIR),up)

seed:
	@test -f "$(ENV_FILE)" || { echo "缺少 $(ENV_FILE)，请复制 deploy/.env.example 为 deploy/.env"; exit 1; }
	ENV_FILE="$(ENV_FILE)" sh deploy/seed.sh

# 只创建 M200000–M200099 和零余额账户。入账脚本属于 QA-02，这里不调用。
seed-load:
	@test -f "$(ENV_FILE)" || { echo "缺少 $(ENV_FILE)，请复制 deploy/.env.example 为 deploy/.env"; exit 1; }
	ENV_FILE="$(ENV_FILE)" SEED_LOAD=1 sh deploy/seed.sh

e2e:
	@echo "e2e 由 QA-04 实现"
	@exit 1

chaos:
	@echo "chaos 由 QA-01 实现"
	@exit 1

load:
	@echo "load 由 QA-02 实现"
	@exit 1
