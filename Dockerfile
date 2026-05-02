FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /booking-service ./cmd/booking

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /booking-service /usr/local/bin/booking-service
COPY config/local.yml /app/config/local.yml

EXPOSE 60017

ENTRYPOINT ["booking-service"]
CMD ["-config", "/app/config/local.yml"]
