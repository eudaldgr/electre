using System.Net;
using System.Net.WebSockets;
using Electre.Bitcoin;
using Electre.Database;
using Electre.Indexer;
using Electre.Metrics;
using Electre.Protocol;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBitcoin;

namespace Electre.Server;

/// <summary>
///     WebSocket server that accepts Electrum client connections over WS/WSS.
/// </summary>
public sealed class WebSocketServer : BackgroundService
{
    private readonly Config _config;
    private readonly IOptions<Config> _configOptions;
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Network _network;
    private readonly RpcClient _rpc;
    private readonly Store _store;
    private readonly SubscriptionManager _subscriptions;
    private readonly Syncer _syncer;
    private HttpListener? _listener;

    public WebSocketServer(
        Store store,
        RpcClient rpc,
        Syncer syncer,
        SubscriptionManager subscriptions,
        IOptions<Config> config,
        Network network,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger("WebSocket");
        _loggerFactory = loggerFactory;
        _store = store;
        _rpc = rpc;
        _syncer = syncer;
        _subscriptions = subscriptions;
        _config = config.Value;
        _configOptions = config;
        _network = network;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _listener = new HttpListener();

        _listener.Prefixes.Add($"http://+:{_config.WsPort}/");
        if (_config.SslEnabled)
            _listener.Prefixes.Add($"https://+:{_config.WssPort}/");

        try
        {
            _listener.Start();
        }
        catch (HttpListenerException ex)
        {
            _logger.LogError(ex,
                "Failed to start WebSocket listener. On Linux, use: sudo setcap cap_net_bind_service=+ep <binary>");
            return;
        }

        _logger.LogInformation("WebSocket listening on port {WsPort}", _config.WsPort);
        if (_config.SslEnabled)
            _logger.LogInformation("Secure WebSocket listening on port {WssPort}", _config.WssPort);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var context = await _listener.GetContextAsync().WaitAsync(ct);
                if (context.Request.IsWebSocketRequest)
                {
                    _ = HandleWebSocketAsync(context, ct);
                }
                else
                {
                    context.Response.StatusCode = 400;
                    context.Response.Close();
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("WebSocket listener cancelled");
        }
        catch (HttpListenerException) when (ct.IsCancellationRequested)
        {
            _logger.LogDebug("WebSocket listener stopped");
        }
        finally
        {
            _listener.Stop();
        }
    }

    private async Task HandleWebSocketAsync(HttpListenerContext httpContext, CancellationToken ct)
    {
        var remoteIp = httpContext.Request.RemoteEndPoint?.Address;

        WebSocketContext wsContext;
        try
        {
            wsContext = await httpContext.AcceptWebSocketAsync(null);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WebSocket accept failed");
            httpContext.Response.StatusCode = 500;
            httpContext.Response.Close();
            return;
        }

        var methods = new Methods(_store, _rpc, _syncer, _subscriptions, _configOptions, _network, _loggerFactory);
        var session = new WebSocketSession(
            wsContext.WebSocket,
            methods,
            _subscriptions,
            _loggerFactory,
            _config.EnableRateLimit,
            _config.RateLimitRequestsPerSecond,
            _config.RateLimitBurstSize,
            _config.MaxBatchSize,
            remoteIp);

        AppMetrics.IncrementConnectedClients();
        _logger.LogInformation("[{SessionId}] WebSocket connected from {IP}", session.Id, remoteIp);

        try
        {
            await session.RunAsync(ct);
        }
        finally
        {
            _logger.LogDebug("[{SessionId}] WebSocket disconnected", session.Id);
            AppMetrics.DecrementConnectedClients();
            await session.DisposeAsync();

            if (wsContext.WebSocket.State == WebSocketState.Open)
                try
                {
                    await wsContext.WebSocket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "Server closing",
                        CancellationToken.None);
                }
                catch (Exception ex)
                {
                    _logger.LogTrace(ex, "Error closing WebSocket");
                }
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken ct)
    {
        _listener?.Stop();
        await base.StopAsync(ct);
    }
}