using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.RateLimiting;
using Electre.Metrics;
using Electre.Protocol;
using Microsoft.Extensions.Logging;

namespace Electre.Server;

/// <summary>
///     Represents a WebSocket client session handling JSON-RPC requests and managing subscriptions.
/// </summary>
public sealed class WebSocketSession : IClientSession, IAsyncDisposable
{
    private const int MaxPendingNotifications = 1000;
    private const int WriteTimeoutMs = 5000;
    private readonly ILogger _logger;
    private readonly int _maxBatchSize;
    private readonly Methods _methods;
    private readonly RateLimiter? _rateLimiter;
    private readonly SubscriptionManager _subscriptions;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    private readonly WebSocket _ws;
    private volatile bool _disposed;
    private bool _hasNegotiated;

    private int _pendingCount;

    public WebSocketSession(
        WebSocket ws,
        Methods methods,
        SubscriptionManager subscriptions,
        ILoggerFactory loggerFactory,
        bool enableRateLimit,
        int requestsPerSecond,
        int burstSize,
        int maxBatchSize,
        IPAddress? remoteIp)
    {
        _logger = loggerFactory.CreateLogger("WsSession");
        _ws = ws;
        _methods = methods;
        _subscriptions = subscriptions;
        _maxBatchSize = maxBatchSize > 0 ? maxBatchSize : 345;
        RemoteIp = remoteIp;

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

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return default;

        _disposed = true;
        _subscriptions.UnsubscribeAll(this);
        _rateLimiter?.Dispose();
        _writeLock.Dispose();
        return default;
    }

    /// <inheritdoc />
    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];

    /// <inheritdoc />
    public ConcurrentDictionary<string, byte> SubscribedScripthashes { get; } = new();

    /// <inheritdoc />
    public bool SubscribedToHeaders { get; set; }

    /// <inheritdoc />
    public IPAddress? RemoteIp { get; }

    /// <inheritdoc />
    public bool IsSlowClient { get; private set; }

    /// <inheritdoc />
    public string? ClientVersion { get; set; }

    /// <inheritdoc />
    public string? ProtocolVersion { get; set; }

    /// <inheritdoc />
    public async Task SendNotificationAsync(JsonRpcNotification notification)
    {
        if (_disposed || IsSlowClient || _ws.State != WebSocketState.Open)
            return;

        var json = JsonRpcSerializer.Serialize(notification);
        var bytes = Encoding.UTF8.GetBytes(json);

        var currentPending = Interlocked.Increment(ref _pendingCount);
        if (currentPending > MaxPendingNotifications)
        {
            Interlocked.Decrement(ref _pendingCount);
            IsSlowClient = true;
            _logger.LogWarning("[{SessionId}] WebSocket client too slow, dropping notifications", Id);
            return;
        }

        var acquired = await _writeLock.WaitAsync(WriteTimeoutMs);
        if (!acquired)
        {
            Interlocked.Decrement(ref _pendingCount);
            IsSlowClient = true;
            _logger.LogWarning("[{SessionId}] WebSocket write timeout, marking as slow client", Id);
            return;
        }

        try
        {
            using var cts = new CancellationTokenSource(WriteTimeoutMs);
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, cts.Token);
        }
        catch (OperationCanceledException)
        {
            IsSlowClient = true;
            _logger.LogWarning("[{SessionId}] WebSocket write timed out, marking as slow client", Id);
        }
        finally
        {
            Interlocked.Decrement(ref _pendingCount);
            _writeLock.Release();
        }
    }

    /// <summary>
    ///     Runs the WebSocket session loop, reading and processing JSON-RPC messages.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var buffer = new byte[65536];

        try
        {
            while (_ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var result = await _ws.ReceiveAsync(buffer, ct);
                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count).Trim();
                    if (!string.IsNullOrWhiteSpace(message))
                        await ProcessMessageAsync(message);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("WebSocket session cancelled");
        }
        catch (WebSocketException)
        {
            _logger.LogDebug("WebSocket connection closed");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[{SessionId}] WebSocket session error", Id);
        }
    }

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
            await SendRawAsync("[" + string.Join(",", responses) + "]");
    }

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

        if (request.Method != "server.version" && !_hasNegotiated)
            return JsonRpcResponse.Failure(GetId(request.Id), JsonRpcErrorCodes.ServerError,
                "server.version must be called before any other method");

        if (request.Method == "server.version" && _hasNegotiated)
            return JsonRpcResponse.Failure(GetId(request.Id), JsonRpcErrorCodes.ServerError,
                "server.version already called");

        if (request.Method == "server.version")
            _hasNegotiated = true;

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

    private async Task SendResponseAsync(JsonRpcResponse response)
    {
        var json = JsonRpcSerializer.Serialize(response);
        await SendRawAsync(json);
    }

    private async Task SendRawAsync(string raw)
    {
        if (_disposed || _ws.State != WebSocketState.Open)
            return;

        var bytes = Encoding.UTF8.GetBytes(raw);

        await _writeLock.WaitAsync();
        try
        {
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }
        finally
        {
            _writeLock.Release();
        }
    }
}