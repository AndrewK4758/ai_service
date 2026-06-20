# Build Stage
FROM golang:alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the Go application.
# CGO_ENABLED=0 builds a perfectly statically linked binary with zero C/C++ dependencies.
RUN CGO_ENABLED=0 GOOS=linux go build -o /ai_service_bin ./cmd/server

# Final Stage (Runtime)
FROM alpine:latest

# We only need basic certificates and poppler-utils for pdf-to-image conversion
RUN apk --no-cache add ca-certificates poppler-utils

WORKDIR /app

# Copy the static binary from the builder
COPY --from=builder /ai_service_bin .

EXPOSE 50051

ENTRYPOINT ["./ai_service_bin"]
