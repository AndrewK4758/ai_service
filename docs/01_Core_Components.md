# Core Components

The Local AI Document Processing Service is a highly-concurrent, 100% local Go microservice. It processes documents (PDFs, DOCX, and Images) and uses local AI models (via OpenAI-compatible endpoints) to extract structured JSON metadata. It serves as an asynchronous event-driven gateway.

## Key Components

### 1. Smart Router
Receives files via gRPC from clients (e.g., a .NET app), assigns them an `app_id` and `correlation_id`, instantly returns a `queued` status, and places the job onto an isolated Go channel.

### 2. Worker Pool
Multiple Go coroutines concurrently process the jobs.
- **Text Documents** (DOCX, text PDFs): Uses pure-Go libraries to instantly parse text.
- **Image Documents** (Image PDFs, PNGs): Converts pages/files to images for vision models.

### 3. AI Engine Integration
Communicates with a local OpenAI-compatible AI instance (like Ollama, vLLM, or LMStudio) to process data in parallel and extract strict JSON metadata.

### 4. NATS JetStream Publisher
Receives the final JSON from the workers and reliably pushes it to the waiting client applications on topics like `ai.processed.<app_id>`, ensuring robust and guaranteed delivery.
