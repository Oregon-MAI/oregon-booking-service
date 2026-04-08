FROM golang:1.26-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o main ./cmd/summator/main.go

FROM golang:1.26-alpine

COPY --from=builder /src/main /test/main
ENTRYPOINT [ "/test/main" ]
