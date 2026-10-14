FROM golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS builder

ARG APP
WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# timetzdata 把时区数据编进二进制。运行镜像不安装包，避免破坏 digest 锁定的可复现构建。
RUN test -n "$APP" \
	&& CGO_ENABLED=0 go build -tags timetzdata -trimpath -ldflags='-s -w' -o "/out/$APP" "./cmd/$APP" \
	&& cp "/out/$APP" /out/app

FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1

RUN addgroup -S -g 65532 nonroot \
	&& adduser -S -D -H -u 65532 -G nonroot nonroot
COPY --from=builder /out/app /usr/local/bin/app
USER nonroot
ENTRYPOINT ["/usr/local/bin/app"]
