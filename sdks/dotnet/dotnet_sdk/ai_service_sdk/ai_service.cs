using Document.Processor.V1;
using Google.Protobuf;
using Grpc.Net.Client;

namespace ai_service_sdk;

public sealed class AI_Service : IDisposable
{
    private GrpcChannel? _channel;
    private DocumentProcessor.DocumentProcessorClient? _client;

    public void Initialize(string address = "http://localhost:50051")
    {
        if (_channel is not null)
        {
            throw new InvalidOperationException("AI_Service is already initialized.");
        }

        _channel = GrpcChannel.ForAddress(address);
        _client = new DocumentProcessor.DocumentProcessorClient(_channel);
    }

    public async Task<ProcessDocumentResponse> ProcessDocumentAsync(
        byte[] fileContent,
        string fileName,
        string appId,
        ModelChoice modelChoice = ModelChoice.Local,
        string? documentType = null,
        IEnumerable<string>? targetFields = null,
        string? jsonSchema = null,
        string? customInstructions = null,
        string? correlationId = null,
        CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var request = new ProcessDocumentRequest
        {
            FileContent = ByteString.CopyFrom(fileContent),
            FileName = fileName,
            ModelChoice = modelChoice,
            AppId = appId,
            DocumentType = documentType ?? string.Empty,
            JsonSchema = jsonSchema ?? string.Empty,
            CustomInstructions = customInstructions ?? string.Empty,
            CorrelationId = correlationId ?? string.Empty,
        };

        if (targetFields is not null)
        {
            request.TargetFields.AddRange(targetFields);
        }

        return await _client!.ProcessDocumentAsync(request, cancellationToken: cancellationToken);
    }

    public async Task<ProcessDocumentResponse> ProcessDocumentAsync(
        ProcessDocumentRequest request,
        CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        return await _client!.ProcessDocumentAsync(request, cancellationToken: cancellationToken);
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _channel = null;
        _client = null;
    }

    private void EnsureInitialized()
    {
        if (_client is null)
        {
            throw new InvalidOperationException("Call Initialize() before processing documents.");
        }
    }
}
