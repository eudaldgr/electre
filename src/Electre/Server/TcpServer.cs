using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
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
///     TCP/SSL server that accepts Electrum wallet client connections and manages their sessions.
///     Implements dual-listener architecture for both plain TCP and SSL/TLS connections.
/// </summary>
public sealed class TcpServer : BackgroundService, IAsyncDisposable
{
    private const int ReadBufferSize = 65536;
    private readonly FileSystemWatcher? _certWatcher;
    private readonly Config _config;
    private readonly IOptions<Config> _configOptions;
    private readonly ConcurrentDictionary<string, int> _connectionsByIp = new();

    private readonly IPEndPoint _endpoint;
    private readonly List<TcpListener> _listeners = [];
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Network _network;
    private readonly RpcClient _rpc;
    private readonly List<Session> _sessions = [];
    private readonly object _sessionsLock = new();
    private readonly IPEndPoint? _sslEndpoint;
    private readonly Store _store;
    private readonly SubscriptionManager _subscriptions;
    private readonly Syncer _syncer;
    private volatile X509Certificate2? _certificate;
    private bool _isAccepting = true;
    private CancellationTokenSource? _serverCts;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TcpServer" /> class.
    /// </summary>
    /// <param name="endpoint">The TCP endpoint to listen on.</param>
    /// <param name="store">The blockchain data store.</param>
    /// <param name="rpc">The Bitcoin Core RPC client.</param>
    /// <param name="syncer">The blockchain synchronizer.</param>
    /// <param name="subscriptions">The subscription manager for client notifications.</param>
    /// <param name="config">The server configuration options.</param>
    /// <param name="network">The Bitcoin network (mainnet, testnet, etc.).</param>
    /// <param name="loggerFactory">The logger factory for creating loggers.</param>
    public TcpServer(
        IPEndPoint endpoint,
        Store store,
        RpcClient rpc,
        Syncer syncer,
        SubscriptionManager subscriptions,
        IOptions<Config> config,
        Network network,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger("Server");
        _loggerFactory = loggerFactory;
        _endpoint = endpoint;
        _store = store;
        _rpc = rpc;
        _syncer = syncer;
        _subscriptions = subscriptions;
        _config = config.Value;
        _configOptions = config;
        _network = network;

        if (_config.SslEnabled && !string.IsNullOrWhiteSpace(_config.SslCertPath))
        {
            _certificate = X509CertificateLoader.LoadPkcs12FromFile(_config.SslCertPath, _config.SslCertPassword);
            _sslEndpoint = new IPEndPoint(_endpoint.Address, _config.SslPort);

            // Watch for certificate file changes to enable hot-reload
            var certDir = Path.GetDirectoryName(Path.GetFullPath(_config.SslCertPath));
            var certFile = Path.GetFileName(_config.SslCertPath);
            if (certDir is not null)
            {
                _certWatcher = new FileSystemWatcher(certDir, certFile)
                {
                    NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.CreationTime,
                    EnableRaisingEvents = true
                };
                _certWatcher.Changed += OnCertificateFileChanged;
                _certWatcher.Created += OnCertificateFileChanged;
            }
        }
    }

    /// <summary>
    ///     Gets the current number of active client sessions.
    /// </summary>
    public int SessionCount
    {
        get
        {
            lock (_sessionsLock)
            {
                return _sessions.Count;
            }
        }
    }

    /// <summary>
    ///     Asynchronously disposes the server and releases all resources.
    /// </summary>
    /// <returns>A value task representing the asynchronous disposal.</returns>
    public async ValueTask DisposeAsync()
    {
        _certWatcher?.Dispose();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await StopAsync(cts.Token);
        _serverCts?.Dispose();
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _serverCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tasks = new List<Task>();

        tasks.Add(RunListenerAsync(_endpoint, false, _serverCts.Token));

        if (_sslEndpoint is not null && _certificate is not null)
            tasks.Add(RunListenerAsync(_sslEndpoint, true, _serverCts.Token));

        await Task.WhenAll(tasks);
    }

    /// <summary>
    ///     Stops the server and gracefully disconnects all active sessions.
    /// </summary>
    /// <param name="ct">The cancellation token to observe.</param>
    /// <returns>A task representing the asynchronous stop operation.</returns>
    public override async Task StopAsync(CancellationToken ct)
    {
        _isAccepting = false;
        _logger.LogInformation("Stopping server, draining {SessionCount} sessions...", SessionCount);

        foreach (var listener in _listeners.ToArray())
            try
            {
                listener.Stop();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error stopping TCP listener");
            }

        _serverCts?.Cancel();

        // Wait for sessions to drain or cancellation
        while (SessionCount > 0 && !ct.IsCancellationRequested)
            await Task.Delay(100, ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        if (SessionCount > 0)
        {
            _logger.LogWarning("Timeout waiting for sessions to drain, forcing disconnect of {Count} sessions",
                SessionCount);
            await ForceDisconnectAllAsync();
        }

        _logger.LogInformation("Server stopped");
    }

    /// <summary>
    ///     Forcefully disconnects all active sessions without waiting for graceful shutdown.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task ForceDisconnectAllAsync()
    {
        Session[] sessions;
        lock (_sessionsLock)
        {
            sessions = _sessions.ToArray();
            _sessions.Clear();
        }

        foreach (var session in sessions)
            try
            {
                await session.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error disposing session during shutdown");
            }
    }

    /// <summary>
    ///     Runs a TCP listener on the specified endpoint, accepting client connections.
    /// </summary>
    /// <param name="endpoint">The endpoint to listen on.</param>
    /// <param name="useSsl">Whether to use SSL/TLS for this listener.</param>
    /// <param name="ct">The cancellation token to observe.</param>
    /// <returns>A task representing the listener loop.</returns>
    private async Task RunListenerAsync(IPEndPoint endpoint, bool useSsl, CancellationToken ct)
    {
        var listener = new TcpListener(endpoint);
        lock (_listeners)
        {
            _listeners.Add(listener);
        }

        listener.Start();
        var protocol = useSsl ? "SSL" : "TCP";
        _logger.LogInformation("Listening on {Endpoint} ({Protocol})", endpoint, protocol);

        try
        {
            while (!ct.IsCancellationRequested && _isAccepting)
            {
                var client = await listener.AcceptTcpClientAsync(ct);
                if (_isAccepting)
                    _ = HandleClientAsync(client, useSsl, ct);
                else
                    client.Close();
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("Listener cancelled");
        }
        catch (SocketException)
        {
            _logger.LogDebug("Listener socket closed");
        }
        finally
        {
            lock (_listeners)
            {
                _listeners.Remove(listener);
            }

            try
            {
                listener.Stop();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error stopping listener in cleanup");
            }
        }
    }

    /// <summary>
    ///     Handles a newly accepted client connection, establishing a session and processing requests.
    /// </summary>
    /// <param name="client">The accepted TCP client.</param>
    /// <param name="useSsl">Whether the connection uses SSL/TLS.</param>
    /// <param name="ct">The cancellation token to observe.</param>
    /// <returns>A task representing the client session.</returns>
    private async Task HandleClientAsync(TcpClient client, bool useSsl, CancellationToken ct)
    {
        if (!_isAccepting)
        {
            client.Close();
            return;
        }

        var remoteIp = GetRemoteIp(client);
        if (remoteIp is not null)
        {
            var currentCount = _connectionsByIp.AddOrUpdate(remoteIp, 1, (_, c) => c + 1);
            if (currentCount > _config.MaxConnectionsPerIp)
            {
                _connectionsByIp.AddOrUpdate(remoteIp, 0, (_, c) => c - 1);
                _logger.LogWarning("Per-IP limit exceeded for {IP} ({Count}/{Max})", remoteIp, currentCount,
                    _config.MaxConnectionsPerIp);
                client.Close();
                return;
            }
        }

        Stream stream;

        try
        {
            if (useSsl && _certificate is not null)
            {
                var sslStream = new SslStream(client.GetStream(), false);
                await sslStream.AuthenticateAsServerAsync(_certificate, false, false);
                stream = sslStream;
            }
            else
            {
                stream = client.GetStream();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SSL handshake failed");
            DecrementIpCount(remoteIp);
            client.Close();
            return;
        }

        var methods = new Methods(_store, _rpc, _syncer, _subscriptions, _configOptions, _network, _loggerFactory);
        var session = new Session(
            client,
            stream,
            methods,
            _subscriptions,
            _loggerFactory,
            _config.EnableRateLimit,
            _config.RateLimitRequestsPerSecond,
            _config.RateLimitBurstSize,
            _config.MaxRequestLineBytes,
            _config.MaxBatchSize,
            useSsl);

        lock (_sessionsLock)
        {
            if (_sessions.Count >= _config.MaxClients)
            {
                DecrementIpCount(remoteIp);
                client.Close();
                return;
            }

            _sessions.Add(session);
        }

        AppMetrics.IncrementConnectedClients();

        var remoteEndpoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        var protocol = useSsl ? "SSL" : "TCP";
        _logger.LogInformation("[{SessionId}] Connected from {Endpoint} ({Protocol})", session.Id, remoteEndpoint,
            protocol);

        try
        {
            await session.RunAsync(ct);
        }
        finally
        {
            _logger.LogDebug("[{SessionId}] Disconnected", session.Id);

            lock (_sessionsLock)
            {
                _sessions.Remove(session);
            }

            AppMetrics.DecrementConnectedClients();
            DecrementIpCount(remoteIp);
            await session.DisposeAsync();
        }
    }

    /// <summary>
    ///     Extracts the remote IP address from a TCP client connection.
    /// </summary>
    /// <param name="client">The TCP client.</param>
    /// <returns>The remote IP address as a string, or null if it cannot be determined.</returns>
    private static string? GetRemoteIp(TcpClient client)
    {
        if (client.Client.RemoteEndPoint is IPEndPoint ep)
            return ep.Address.ToString();
        return null;
    }

    /// <summary>
    ///     Decrements the connection count for a specific IP address.
    /// </summary>
    /// <param name="ip">The IP address to decrement, or null to skip.</param>
    private void DecrementIpCount(string? ip)
    {
        if (ip is not null)
            _connectionsByIp.AddOrUpdate(ip, 0, (_, c) => Math.Max(0, c - 1));
    }

    /// <summary>
    ///     Handles certificate file changes by reloading the SSL certificate.
    ///     New connections will use the updated certificate; existing connections are unaffected.
    /// </summary>
    private void OnCertificateFileChanged(object sender, FileSystemEventArgs e)
    {
        try
        {
            // Debounce: wait for file write to complete
            Thread.Sleep(1000);
            var newCert = X509CertificateLoader.LoadPkcs12FromFile(_config.SslCertPath!, _config.SslCertPassword);
            var old = _certificate;
            _certificate = newCert;
            old?.Dispose();
            _logger.LogInformation("SSL certificate reloaded from {Path}", _config.SslCertPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to reload SSL certificate");
        }
    }
}