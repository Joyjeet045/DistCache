# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# The module has no third-party dependencies, so the module graph is just
# go.mod. Copy it first for better layer caching, then the sources.
COPY go.mod ./
COPY . .

ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags "-s -w" -o /out/cache-server ./cmd/cache-server \
    && go build -trimpath -ldflags "-s -w" -o /out/cache-cli ./cmd/cache-cli

# ---- runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache wget \
    && adduser -D -u 10001 distcache \
    && mkdir -p /data && chown distcache /data

COPY --from=build /out/cache-server /usr/local/bin/cache-server
COPY --from=build /out/cache-cli /usr/local/bin/cache-cli

USER distcache
VOLUME ["/data"]
# 6380 = RESP client port, 9121 = Prometheus metrics, 7000 = replication.
EXPOSE 6380 9121 7000

ENTRYPOINT ["cache-server"]
CMD ["-addr", ":6380", "-metrics-addr", ":9121"]
