# Local AI Document Processing Service

This is a blazing-fast, 100% local, air-gapped Go microservice that processes documents (PDFs, DOCX, and Images) and uses local AI models to extract structured JSON metadata. 

It acts as a highly efficient gRPC gateway between a .NET frontend/API and a local Ollama AI instance.

## 🏗 Architecture
1. **The Client (.NET / C#)** sends a file as raw bytes via gRPC, along with a dynamic prompt and a strict JSON Schema.
2. **The Smart Router (Go)** receives the file:
   - If it's a `.docx` or `.pdf`, it uses pure-Go libraries to instantly parse the native text.
   - If it's an image, it Base64 encodes it.
3. **The AI Engine (Ollama)** receives the extracted text/image from the Go service, applies the strict JSON Schema formatting, and returns perfectly structured metadata.

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
Once Ollama is installed, open a terminal and pull the models we use in this service. 

**The Text Model** (For analyzing extracted text from PDFs and DOCX files):
```bash
ollama pull gemma4:12b
```

**The Vision Model** (For analyzing raw images like PNGs/JPGs):
```bash
ollama pull glm-ocr
```

---

## 🚀 2. Launching the Go Service

This service is fully containerized using Docker, making deployment incredibly easy and ensuring zero system-level dependencies are required.

1. Open a terminal in the root of this project.
2. Build and start the Docker container:
   ```bash
   docker compose up --build
   ```

Because the `docker-compose.yml` uses `network_mode: "host"`, the Docker container will share your host machine's network stack. It will seamlessly connect to your local Ollama instance without any firewall or port mapping issues, and expose the gRPC server natively on port `50051`.

---

## 🧪 3. Testing the Pipeline

You don't need a .NET app to verify the service works! The repository includes a pure Go test client that mimics exactly what your .NET application will do.

While the Docker container is running, open a **new terminal** and run the test script against any document on your computer:

```bash
# Test a PDF or Word Document
go run scripts/test_client/main.go /path/to/some/invoice.pdf

# Test an Image
go run scripts/test_client/main.go /path/to/some/screenshot.png
```

You should see the raw JSON metadata output directly in your terminal. If you want to modify the prompt or the expected JSON Schema, edit the variables inside `scripts/test_client/main.go`.

---

## 🔌 4. Connecting Your .NET Application

Connecting your C# application to this service is fully automated via gRPC Protobufs.

1. **Copy the Contract:** Copy the `proto/processor.proto` file into a `Protos/` folder in your .NET project.
2. **Install Packages:** Run the following in your .NET project:
   ```bash
   dotnet add package Grpc.Net.Client
   dotnet add package Google.Protobuf
   dotnet add package Grpc.Tools
   ```
3. **Update `.csproj`:** Add the protobuf generation instruction inside your `.csproj`:
   ```xml
   <ItemGroup>
     <Protobuf Include="Protos\processor.proto" GrpcServices="Client" />
   </ItemGroup>
   ```
4. **Call the Service in C#:**
   ```csharp
   using Grpc.Net.Client;
   using Document.Processor.V1;
   using Google.Protobuf;

   // Connect to the Go Docker container
   using var channel = GrpcChannel.ForAddress("http://localhost:50051");
   var client = new DocumentProcessor.DocumentProcessorClient(channel);

   // Read your file
   var fileBytes = await File.ReadAllBytesAsync("invoice.pdf");

   // Build the request with your dynamic schema
   var request = new ProcessDocumentRequest
   {
       FileContent = ByteString.CopyFrom(fileBytes),
       FileName = "invoice.pdf",
       ModelChoice = ModelChoice.Local,
       DocumentType = "Invoice",
       CustomInstructions = "Extract all totals accurately.",
       JsonSchema = "{\"type\": \"object\", ...}" 
   };

   // Execute!
   var response = await client.ProcessDocumentAsync(request);
   Console.WriteLine(response.MetadataJson);
   ```
