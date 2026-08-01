# How To Implement and Use

## Prerequisites
Because this architecture runs 100% locally, you must run an OpenAI-compatible server on your host machine (like Ollama).

1. Install Ollama:
   ```bash
   curl -fsSL https://ollama.com/install.sh | sh
   ```
2. Pull the AI Models:
   - **Text Model:** `ollama pull gemma4:2b`
   - **Vision Model:** `ollama pull glm-ocr`

## Configuration
Create a `.env` file in the project root:
```env
OPENAI_BASE_URL=http://127.0.0.1:11434/v1
OPENAI_TEXT_MODEL=gemma4:2b
OPENAI_VISION_MODEL=glm-ocr
OPENAI_NUM_PARALLEL=6
NATS_URL=nats://127.0.0.1:4222
SERVER_PORT=11111
APP_QUEUE_SIZE=100
WORKER_POOL_SIZE=5
```

## Running the Service
**Using Docker (Recommended):**
```bash
docker-compose up -d --build
```

**Local Development (Linux/macOS):**
```bash
export OPENAI_BASE_URL=http://127.0.0.1:11434/v1
export NATS_URL=nats://127.0.0.1:4222
export SERVER_PORT=11111
go run cmd/server/main.go
```

## Service Discovery & Dynamic Model Routing
The service dynamically maps physical LLM model IDs from incoming NATS JetStream subjects (e.g., `worker.doc.chunking`).
- Trailing token (`chunking`) is normalized (`CHUNKING`).
- Environment variable `MODEL_ROUTE_CHUNKING` is queried.
- Fallback hierarchy: `MODEL_ROUTE_<UPPERCASE_TOKEN>` -> `MODEL_ROUTE_DEFAULT` -> Default `OPENAI_TEXT_MODEL`.

Example:
```env
MODEL_ROUTE_CHUNKING=gemma4-extended
MODEL_ROUTE_DEFAULT=gemma4:e4b-extended
```
