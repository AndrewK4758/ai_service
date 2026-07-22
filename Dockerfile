# Build Stage
FROM golang:alpine AS builder

WORKDIR /app

COPY --from=shared_protos . /app/shared_protos
COPY --from=shared_utils . /app/shared_utils
COPY . /app/ai_service
WORKDIR /app/ai_service
RUN go mod edit -replace github.com/AndrewK4758/shared_protos=../shared_protos

# Download dependencies
RUN go mod edit -replace github.com/AndrewK4758/shared_utils=../shared_utils
RUN go mod download

# Build the Go application
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
