# Local AI Document Processing Service

This is a blazing-fast, highly-concurrent, 100% local Go microservice that processes documents (PDFs, DOCX, and Images) and uses local AI models to extract structured JSON metadata. 

It acts as an **asynchronous event-driven** gateway between your frontend/API and a local Ollama AI instance, utilizing **NATS JetStream** for robust, guaranteed delivery.

## 🏗 Architecture
1. **The Client (.NET / C#)** sends a file or batch of files via gRPC, providing an `app_id` and an optional `correlation_id`.
2. **The Smart Router (Go)** receives the files and instantly returns a `queued` status to the client, placing the job onto an isolated Go channel specific to that `app_id`.
3. **The Worker Pool** (running multiple Go coroutines per app) picks up the jobs concurrently.
   - If it's a `.docx` or text `.pdf`, it uses pure-Go libraries to instantly parse the text.
   - If it's an image `.pdf`, it converts it to images.
4. **The AI Engine (Ollama)** processes the data in parallel and extracts strict JSON metadata.
5. **NATS JetStream** receives the final JSON from the workers and reliably pushes it to the waiting client applications on topics like `ai.processed.<app_id>`.

---

## 🛠 1. Prerequisites & Installation

### A. Install Ollama
Because this architecture runs 100% locally on your machine's GPU, you must install Ollama natively on your host machine.

**For Linux (Arch/Ubuntu/etc):**
```bash
curl -fsSL https://ollama.com/install.sh | sh
```
*(Note: If you use a different OS, visit [ollama.com/download](https://ollama.com/download))*

### B. Pull the AI Models
Once Ollama is installed, open a terminal and pull the fast, lightweight models we use in this service. 

**The Text Model** (For analyzing extracted text from PDFs and DOCX files):
```bash
ollama pull gemma4:2b
```

**The Vision Model** (For analyzing raw images like PNGs/JPGs):
```bash
ollama pull glm-ocr
```

### C. Enable Ollama Concurrency
To allow Ollama to process multiple files simultaneously, you must configure it to handle parallel requests.
If running Ollama via systemd:
```bash
sudo systemctl edit ollama.service
```
Add the following to the override file:
```ini
[Service]
Environment="OLLAMA_NUM_PARALLEL=5"
```
Restart the service: `sudo systemctl restart ollama`.

---

## 🚀 2. Configuration & Launch

### Docker (All Platforms - Recommended)
1. Ensure the `.env` file exists in the root of the project:
   ```env
   OLLAMA_HOST=http://127.0.0.1:11434
   OLLAMA_TEXT_MODEL=gemma4:2b
   OLLAMA_VISION_MODEL=glm-ocr
   OLLAMA_NUM_PARALLEL=5
   NATS_URL=nats://127.0.0.1:4222
   SERVER_PORT=50051
   APP_QUEUE_SIZE=100
   WORKER_POOL_SIZE=5
   ```
2. Build and start the Docker container from the project root:
   ```bash
   docker-compose up -d --build
   ```

### Local Development (Linux / macOS)
1. Ensure Ollama and NATS are running locally.
2. Set environment variables and run:
   ```bash
   export OLLAMA_HOST=http://127.0.0.1:11434
   export NATS_URL=nats://127.0.0.1:4222
   export SERVER_PORT=50051
   go run cmd/server/main.go
   ```

### Local Development (Windows PowerShell)
1. Ensure Ollama and NATS are running locally.
2. Set environment variables and run:
   ```powershell
   $env:OLLAMA_HOST="http://127.0.0.1:11434"
   $env:NATS_URL="nats://127.0.0.1:4222"
   $env:SERVER_PORT="50051"
   go run cmd\server\main.go
   ```

---

## 🧪 3. Testing the Parallel Pipeline

You don't need a .NET app to verify the service works! The repository includes a pure Go test client that mimics an event-driven application.

While the Docker container is running, open a **new terminal** and run the test script against a single document or an **entire directory of documents**.

```bash
# Process a single file
go run scripts/test_client/main.go /path/to/some/invoice.pdf

# Process a batch of files in parallel
go run scripts/test_client/main.go /path/to/my_invoices/
```

The test client will:
1. Connect to NATS JetStream.
2. Instantly dispatch all files to the Go server via gRPC.
3. Wait and listen to the NATS channel to receive the parallel AI responses!

---

## 🔌 4. Connecting Your .NET Application

Connecting your C# application involves two steps: sending the gRPC request, and listening for the NATS response.

### Step 1: Install Packages
```bash
dotnet add package Grpc.Net.Client
dotnet add package Google.Protobuf
dotnet add package Grpc.Tools
dotnet add package NATS.Net  # For receiving the async response
```

### Step 2: Send the Request (gRPC)
```csharp
using Grpc.Net.Client;
using Document.Processor.V1;
using Google.Protobuf;

// Connect to the Go Docker container
using var channel = GrpcChannel.ForAddress("http://localhost:50051");
var client = new DocumentProcessor.DocumentProcessorClient(channel);

var fileBytes = await File.ReadAllBytesAsync("invoice.pdf");

var request = new ProcessDocumentRequest
{
    FileContent = ByteString.CopyFrom(fileBytes),
    FileName = "invoice.pdf",
    ModelChoice = ModelChoice.Local,
    AppId = "my_dotnet_app", // The routing ID for NATS
    DocumentType = "Invoice",
    JsonSchema = "{\"type\": \"object\", ...}" 
};

// Returns instantly!
var response = await client.ProcessDocumentAsync(request);
Console.WriteLine($"Dispatched with Correlation ID: {response.CorrelationId}");
```

### Step 3: Receive the Result (NATS JetStream)
```csharp
using NATS.Client.Core;
using NATS.Client.JetStream;

await using var nats = new NatsConnection();
var js = new NatsJSContext(nats);

// Listen to the specific topic for your app
var subject = "ai.processed.my_dotnet_app";

await foreach (var msg in js.ConsumeAsync<string>(subject))
{
    Console.WriteLine($"Received AI Processing Result: {msg.Data}");
    await msg.AckAsync(); // Tell JetStream we successfully received it
}
```
