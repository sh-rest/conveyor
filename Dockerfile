# Stage 1: build both binaries
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api  ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# Stage 2: minimal runtime image
FROM gcr.io/distroless/static-debian12

ARG BINARY=api
COPY --from=builder /out/${BINARY} /app

ENTRYPOINT ["/app"]
