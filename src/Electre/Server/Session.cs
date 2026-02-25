using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.RateLimiting;
using Electre.Metrics;
using Electre.Protocol;
using Microsoft.Extensions.Logging;

namespace Electre.Server;

/// <summary>
///     Represents an individual client session handling JSON-RPC requests and managing subscriptions.
/// </summary>
public sealed class Session : IClientSession, IAsyncDisposable
{
    private const int MaxPendingNotifications = 1000;
    private const int WriteTimeoutMs = 5000;

    private readonly TcpClient _client;
    private readonly ILogger _logger;
    private readonly int _maxBatchSize;
    private readonly int _maxRequestLineBytes;
    private readonly Methods _methods;
    private readonly ConcurrentQueue<byte[]> _pendingNotifications = new();
    private readonly RateLimiter? _rateLimiter;
    private readonly Stream _stream;
    private readonly SubscriptionManager _subscriptions;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private volatile bool _disposed;
    private bool _hasNegotiated;
    private int _pendingCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="Session" /> class.
    /// </summary>
    /// <param name="client">The TCP client connection.</param>
    /// <param name="stream">The network stream for reading and writing data.</param>
    /// <param name="methods">The JSON-RPC method handler.</param>
    /// <param name="subscriptions">The subscription manager for notifications.</param>
    /// <param name="loggerFactory">The logger factory for creating loggers.</param>
    /// <param name="enableRateLimit">Whether to enable rate limiting for this session.</param>
    /// <param name="requestsPerSecond">The maximum requests per second allowed.</param>
    /// <param name="burstSize">The maximum burst size for rate limiting.</param>
    /// <param name="isSsl">Whether the connection uses SSL/TLS.</param>
    public Session(
        TcpClient client,
        Stream stream,
        Methods methods,
        SubscriptionManager subscriptions,
        ILoggerFactory loggerFactory,
        bool enableRateLimit,
        int requestsPerSecond,
        int burstSize,
        int maxRequestLineBytes,
        int maxBatchSize,
        bool isSsl = false)
    {
        _logger = loggerFactory.CreateLogger("Session");
        _client = client;
        _stream = stream;
        _methods = methods;
        _subscriptions = subscriptions;
        IsSsl = isSsl;
        _maxRequestLineBytes = Math.Max(1024, maxRequestLineBytes);
        _maxBatchSize = maxBatchSize > 0 ? maxBatchSize : 345;
        RemoteIp = (client.Client.RemoteEndPoint as IPEndPoint)?.Address;

        if (enableRateLimit && requestsPerSecond > 0 && burstSize > 0)
            _rateLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
            {
                TokenLimit = burstSize,
                TokensPerPeriod = requestsPerSecond,
                ReplenishmentPeriod = TimeSpan.FromSeconds(1),
                QueueLimit = 0,
                AutoReplenishment = true
            });
    }

    /// <summary>
    ///     Gets a value indicating whether this session uses SSL/TLS encryption.
    /// </summary>
    public bool IsSsl { get; }

    /// <summary>
    ///     Asynchronously disposes the session and releases all resources.
    /// </summary>
    /// <returns>A value task representing the asynchronous disposal.</returns>
    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return default;

        _disposed = true;

        _subscriptions.UnsubscribeAll(this);

        try
        {
            _stream.Close();
            _client.Close();
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "Error during session dispose");
        }

        _rateLimiter?.Dispose();
        _writeLock.Dispose();
        return default;
    }

    /// <summary>
    ///     Gets the unique session identifier.
    /// </summary>
    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];

    /// <summary>
    ///     Gets the set of scripthashes this session is subscribed to (thread-safe).
    /// </summary>
    public ConcurrentDictionary<string, byte> SubscribedScripthashes { get; } = new();

    /// <summary>
    ///     Gets or sets a value indicating whether this session is subscribed to header notifications.
    /// </summary>
    public bool SubscribedToHeaders { get; set; }

    /// <summary>
    ///     Gets or sets the client version string reported by the Electrum client.
    /// </summary>
    public string? ClientVersion { get; set; }

    /// <summary>
    ///     Gets or sets the protocol version negotiated with the client.
    /// </summary>
    public string? ProtocolVersion { get; set; }

    /// <summary>
    ///     Gets a value indicating whether this client is too slow to receive notifications.
    /// </summary>
    public bool IsSlowClient { get; private set; }

    /// <summary>
    ///     Gets the remote IP address of the client, or null if it cannot be determined.
    /// </summary>
    public IPAddress? RemoteIp { get; }

    /// <summary>
    ///     Sends a JSON-RPC notification to the client with timeout and slow-client detection.
    /// </summary>
    /// <param name="notification">The JSON-RPC notification to send.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    public async Task SendNotificationAsync(JsonRpcNotification notification)
    {
        if (_disposed || IsSlowClient)
            return;

        var json = JsonRpcSerializer.Serialize(notification) + "\n";
        var bytes = Encoding.UTF8.GetBytes(json);

        var currentPending = Interlocked.Increment(ref _pendingCount);
        if (currentPending > MaxPendingNotifications)
        {
            Interlocked.Decrement(ref _pendingCount);
            IsSlowClient = true;
            _logger.LogWarning("[{SessionId}] Client too slow, dropping notifications (pending: {PendingCount})", Id,
                currentPending);
            return;
        }

        var acquired = await _writeLock.WaitAsync(WriteTimeoutMs);
        if (!acquired)
        {
            Interlocked.Decrement(ref _pendingCount);
            IsSlowClient = true;
            _logger.LogWarning("[{SessionId}] Write timeout, marking as slow client", Id);
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(WriteTimeoutMs);
            await _stream.WriteAsync(bytes, cts.Token);
            await _stream.FlushAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            IsSlowClient = true;
            _logger.LogWarning("[{SessionId}] Write timed out, marking as slow client", Id);
        }
        finally
        {
            Interlocked.Decrement(ref _pendingCount);
            _writeLock.Release();
        }
    }

    /// <summary>
    ///     Runs the session loop, reading and processing JSON-RPC requests from the client.
    /// </summary>
    /// <param name="ct">The cancellation token to observe.</param>
    /// <returns>A task representing the session loop.</returns>
    public async Task RunAsync(CancellationToken ct)
    {
        var buffer = new byte[65536];
        var messageBuffer = new StringBuilder();

        try
        {
            while (!ct.IsCancellationRequested && _client.Connected)
            {
                var bytesRead = await _stream.ReadAsync(buffer, ct);
                if (bytesRead == 0)
                    break;

                messageBuffer.Append(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                if (messageBuffer.Length > _maxRequestLineBytes)
                {
                    await SendResponseAsync(JsonRpcResponse.Failure(null, JsonRpcErrorCodes.InvalidRequest,
                        "Request line too large"));
                    _logger.LogWarning("[{SessionId}] Closing client due to oversized request line ({Length} bytes)",
                        Id, messageBuffer.Length);
                    break;
                }

                // Process complete messages (delimited by newline)
                var content = messageBuffer.ToString();
                int newlineIndex;
                while ((newlineIndex = content.IndexOf('\n')) >= 0)
                {
                    var message = content[..newlineIndex].Trim();
                    content = content[(newlineIndex + 1)..];

                    if (!string.IsNullOrWhiteSpace(message))
                        await ProcessMessageAsync(message);
                }

                messageBuffer.Clear();
                messageBuffer.Append(content);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (IOException)
        {
            // Client disconnected
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[{SessionId}] Session error", Id);
        }
    }

    /// <summary>
    ///     Processes a single JSON-RPC message from the client, supporting both single and batch requests.
    /// </summary>
    /// <param name="message">The JSON-RPC message string.</param>
    /// <returns>A task representing the asynchronous processing.</returns>
    private async Task ProcessMessageAsync(string message)
    {
        if (JsonRpcSerializer.IsBatchRequest(message))
        {
            await ProcessBatchAsync(message);
            return;
        }

        var request = JsonRpcSerializer.ParseRequest(message);
        if (request is null)
        {
            await SendResponseAsync(JsonRpcResponse.Failure(null, JsonRpcErrorCodes.ParseError, "Invalid JSON"));
            return;
        }

        var response = await HandleSingleRequestAsync(request);
        if (response is not null)
            await SendResponseAsync(response);
    }

    /// <summary>
    ///     Processes a JSON-RPC batch request (array of requests).
    /// </summary>
    /// <param name="message">The JSON batch message string.</param>
    /// <returns>A task representing the asynchronous batch processing.</returns>
    private async Task ProcessBatchAsync(string message)
    {
        var requests = JsonRpcSerializer.ParseBatchRequest(message);
        if (requests is null || requests.Length == 0)
        {
            await SendResponseAsync(JsonRpcResponse.Failure(null, JsonRpcErrorCodes.ParseError, "Invalid batch"));
            return;
        }

        if (requests.Length > _maxBatchSize)
        {
            await SendResponseAsync(JsonRpcResponse.Failure(null, JsonRpcErrorCodes.InvalidRequest,
                $"Batch too large ({requests.Length} > {_maxBatchSize})"));
            return;
        }

        var responses = new List<string>(requests.Length);
        foreach (var request in requests)
        {
            var response = await HandleSingleRequestAsync(request);
            if (response is not null)
                responses.Add(JsonRpcSerializer.Serialize(response));
        }

        if (responses.Count > 0)
            await SendRawAsync("[" + string.Join(",", responses) + "]\n");
    }

    /// <summary>
    ///     Handles a single JSON-RPC request with rate limiting, negotiation checks, and method dispatch.
    /// </summary>
    /// <param name="request">The parsed JSON-RPC request.</param>
    /// <returns>A JSON-RPC response, or null for notifications that produce no response.</returns>
    private async Task<JsonRpcResponse?> HandleSingleRequestAsync(JsonRpcRequest request)
    {
        if (_rateLimiter is not null)
        {
            using var lease = await _rateLimiter.AcquireAsync();
            if (!lease.IsAcquired)
            {
                _logger.LogWarning("[{SessionId}] Rate limit exceeded", Id);
                AppMetrics.IncrementRateLimited();
                return JsonRpcResponse.Failure(null, JsonRpcErrorCodes.ServerBusy, "Rate limit exceeded");
            }
        }

        AppMetrics.RecordClientRequest(request.Method);

        // Protocol 1.6: server.version must be the first message
        if (request.Method != "server.version" && !_hasNegotiated)
            return JsonRpcResponse.Failure(GetId(request.Id), JsonRpcErrorCodes.ServerError,
                "server.version must be called before any other method");

        if (request.Method == "server.version" && _hasNegotiated)
            return JsonRpcResponse.Failure(GetId(request.Id), JsonRpcErrorCodes.ServerError,
                "server.version already called");

        if (request.Method == "server.version") _hasNegotiated = true;

        try
        {
            var result = await _methods.HandleAsync(request.Method, request.GetParams(), this);
            return JsonRpcResponse.Success(GetId(request.Id), result);
        }
        catch (JsonRpcException ex)
        {
            return JsonRpcResponse.Failure(GetId(request.Id), ex.Code, ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[{SessionId}] Method {Method} error", Id, request.Method);
            return JsonRpcResponse.Failure(GetId(request.Id), JsonRpcErrorCodes.InternalError, ex.Message);
        }
    }

    /// <summary>
    ///     Extracts the request ID from a JSON element, handling various JSON types.
    /// </summary>
    /// <param name="id">The JSON element containing the request ID.</param>
    /// <returns>The request ID as an object (string, long, double, or null).</returns>
    private static object? GetId(JsonElement? id)
    {
        if (id is null || id.Value.ValueKind == JsonValueKind.Undefined)
            return null;

        return id.Value.ValueKind switch
        {
            JsonValueKind.Number => id.Value.TryGetInt64(out var l) ? l : id.Value.GetDouble(),
            JsonValueKind.String => id.Value.GetString(),
            _ => null
        };
    }

    /// <summary>
    ///     Sends a JSON-RPC response to the client.
    /// </summary>
    /// <param name="response">The JSON-RPC response to send.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    private async Task SendResponseAsync(JsonRpcResponse response)
    {
        var json = JsonRpcSerializer.Serialize(response) + "\n";
        var bytes = Encoding.UTF8.GetBytes(json);

        await _writeLock.WaitAsync();
        try
        {
            await _stream.WriteAsync(bytes);
            await _stream.FlushAsync();
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    ///     Sends a raw pre-serialized string to the client, bypassing JSON serialization.
    ///     Used for batch responses where individual items are already serialized.
    /// </summary>
    /// <param name="raw">The raw string to send.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    private async Task SendRawAsync(string raw)
    {
        var bytes = Encoding.UTF8.GetBytes(raw);

        await _writeLock.WaitAsync();
        try
        {
            await _stream.WriteAsync(bytes);
            await _stream.FlushAsync();
        }
        finally
        {
            _writeLock.Release();
        }
    }
}