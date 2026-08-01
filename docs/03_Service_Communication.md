# Service Communication

## Client to Service (gRPC)
Clients interact with the AI Service over gRPC to dispatch jobs asynchronously.
Example (C#):
```csharp
using var channel = GrpcChannel.ForAddress("http://localhost:11111");
var client = new DocumentProcessor.DocumentProcessorClient(channel);
var request = new ProcessDocumentRequest
{
    FileContent = ByteString.CopyFrom(fileBytes),
    FileName = "invoice.pdf",
    ModelChoice = ModelChoice.Local,
    AppId = "my_dotnet_app"
};
var response = await client.ProcessDocumentAsync(request); // Returns instantly
```

## Service to Client (NATS JetStream)
Once processed, the result is published to NATS JetStream on a topic specific to the `app_id`.
Example (C#):
```csharp
var subject = "ai.processed.my_dotnet_app";
await foreach (var msg in js.ConsumeAsync<string>(subject))
{
    Console.WriteLine($"Received Result: {msg.Data}");
    await msg.AckAsync();
}
```

## Data Contracts & No Preemptive Serialization
This service adheres to the strict **No Preemptive Serialization** mandate. 
- JSON-based structural state must be parsed natively using `google.protobuf.Struct`.
- Preemptive stringification is forbidden to prevent schema drift, whitespace corruption, and nested parsing bugs.
- Boundary interfaces rely on typed object mappings rather than dynamic regex or fuzzy key lookups.
