/// <summary>
/// Electre - Electrum Protocol Server entry point.
/// 
/// This file configures the dependency injection container, logging infrastructure,
/// and hosted services for the Electrum protocol server. It uses top-level statements
/// (C# 9+ feature) to initialize the application, register services, and start the host.
/// 
/// The server listens for Electrum protocol clients on a configurable TCP port,
/// synchronizes blockchain data from Bitcoin Core via RPC, and maintains an indexed
/// database of transactions and UTXOs using RocksDB.
/// </summary>

using System.Net;
using Electre;
using Electre.Bitcoin;
using Electre.Database;
using Electre.Health;
using Electre.Indexer;
using Electre.Metrics;
using Electre.Server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBitcoin;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;

var builder = Host.CreateApplicationBuilder(args);

// Configuration
builder.Configuration.AddUserSecrets<Config>(true, true);
builder.Services.Configure<Config>(builder.Configuration);
// Temporarily kept for startup logic before standard DI usage
var config = builder.Configuration.Get<Config>() ?? new Config();

// Configure Host Logging (Serilog)
builder.Services.AddSerilog((sp, loggerConfig) =>
{
    var cfg = sp.GetRequiredService<IOptions<Config>>().Value;
    var loggingCfg = cfg.Logging;

    loggerConfig
        .MinimumLevel.Is(ParseLevel(loggingCfg.MinimumLevel))
        .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Warning)
        .MinimumLevel.Override("Microsoft.Extensions.Hosting", LogEventLevel.Warning);

    if (loggingCfg.EnableConsole)
        loggerConfig.WriteTo.Console(
            ParseLevel(loggingCfg.ConsoleLevel),
            "[{Timestamp:HH:mm:ss} {Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}");

    if (loggingCfg.EnableFile && !string.IsNullOrWhiteSpace(loggingCfg.FilePath))
    {
        var fileLevel = ParseLevel(loggingCfg.FileLevel);
        if (loggingCfg.FileFormat.Equals("json", StringComparison.OrdinalIgnoreCase))
            loggerConfig.WriteTo.File(
                new CompactJsonFormatter(),
                loggingCfg.FilePath,
                fileLevel,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: loggingCfg.RetainedFileCount,
                fileSizeLimitBytes: loggingCfg.FileSizeLimitMb * 1024 * 1024,
                rollOnFileSizeLimit: true);
        else
            loggerConfig.WriteTo.File(
                loggingCfg.FilePath,
                outputTemplate:
                "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] [{SourceContext}] {Message:lj}{NewLine}{Exception}",
                restrictedToMinimumLevel: fileLevel,
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: loggingCfg.RetainedFileCount,
                fileSizeLimitBytes: loggingCfg.FileSizeLimitMb * 1024 * 1024,
                rollOnFileSizeLimit: true);
    }
});

// Network
var network = ResolveNetwork(config.Network);
builder.Services.AddSingleton(network);

// Services
builder.Services.AddSingleton<Store>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<Config>>();
    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
    var configuredPath = opts.Value.DatabasePath;
    var resolvedPath = Path.IsPathRooted(configuredPath)
        ? Path.GetFullPath(configuredPath)
        : Path.GetFullPath(configuredPath, Environment.CurrentDirectory);

    // Resolve once on startup; relative paths are anchored to runtime working directory.
    opts.Value.DatabasePath = resolvedPath;

    loggerFactory.CreateLogger("Startup").LogInformation(
        "DatabasePath resolved from '{ConfiguredPath}' to '{ResolvedPath}' (cwd: {CurrentDirectory})",
        configuredPath,
        resolvedPath,
        Environment.CurrentDirectory);

    return new Store(opts.Value.DatabasePath, opts, loggerFactory);
});

builder.Services.AddSingleton<RpcClient>(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<Config>>().Value;
    return new RpcClient(
        cfg.BitcoinRpcUrl,
        cfg.BitcoinRpcUser,
        cfg.BitcoinRpcPassword,
        sp.GetRequiredService<ILoggerFactory>(),
        cfg.RpcMaxRetries,
        cfg.RpcRetryDelayMs,
        cfg.RpcTimeoutSeconds);
});

builder.Services.AddSingleton<SubscriptionManager>(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<Config>>().Value;
    return new SubscriptionManager(cfg.MaxSubsGlobally, cfg.MaxSubsPerIp);
});

// Caches
builder.Services.AddSingleton(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<Config>>().Value;
    return new UtxoCache(
        sp.GetRequiredService<Store>(),
        sp.GetRequiredService<ILoggerFactory>(),
        cfg.UtxoCacheMaxEntries,
        cfg.UtxoCacheFlushThreshold);
});
builder.Services.AddSingleton(_ => new LruCache<string, string?>(100_000)); // StatusCache

// Components
builder.Services.AddSingleton<BlockValidator>();
builder.Services.AddSingleton<ReorgManager>();

// Syncer - Registered as Singleton and HostedService
builder.Services.AddSingleton<Syncer>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<Syncer>());

// TcpServer - Registered as HostedService with factory for endpoint
builder.Services.AddHostedService<TcpServer>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<Config>>();
    return new TcpServer(
        new IPEndPoint(IPAddress.Any, opts.Value.ServerPort),
        sp.GetRequiredService<Store>(),
        sp.GetRequiredService<RpcClient>(),
        sp.GetRequiredService<Syncer>(),
        sp.GetRequiredService<SubscriptionManager>(),
        opts,
        sp.GetRequiredService<Network>(),
        sp.GetRequiredService<ILoggerFactory>());
});

// Health Checks
builder.Services.AddHealthChecks()
    .AddCheck<BitcoinRpcHealthCheck>("bitcoin_rpc", tags: ["ready", "live"])
    .AddCheck<SyncerHealthCheck>("syncer", tags: ["ready"]);

// MetricsServer
if (config.MetricsEnabled)
    builder.Services.AddHostedService<MetricsServer>();

// WebSocketServer
if (config.WsEnabled)
    builder.Services.AddHostedService<WebSocketServer>();

var host = builder.Build();

var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Electre");
var cfg = host.Services.GetRequiredService<IOptions<Config>>().Value;
var net = host.Services.GetRequiredService<Network>();

logger.LogInformation("Electre - Electrum Protocol Server starting");
logger.LogInformation("Network: {Network}", net.Name);
logger.LogInformation("Database: {DatabasePath}", cfg.DatabasePath);
logger.LogInformation("Port: {ServerPort}", cfg.ServerPort);
if (cfg.SslEnabled)
    logger.LogInformation("SSL Port: {SslPort}", cfg.SslPort);
if (cfg.WsEnabled)
    logger.LogInformation("WebSocket Port: {WsPort}", cfg.WsPort);
if (cfg.MetricsEnabled)
    logger.LogInformation("Metrics Port: {MetricsPort}", cfg.MetricsPort);

try
{
    await host.RunAsync();
}
catch (Exception ex)
{
    logger.LogCritical(ex, "Fatal error");
    throw;
}
finally
{
    logger.LogInformation("Goodbye!");
    await Log.CloseAndFlushAsync();
}

return;

/// <summary>
/// Resolves a network name string to an NBitcoin Network instance.
/// </summary>
/// <param name="networkName">The network name to resolve (e.g., "mainnet", "testnet", "signet", "regtest").</param>
/// <returns>The corresponding NBitcoin Network instance.</returns>
/// <exception cref="InvalidOperationException">Thrown when the network name is not supported.</exception>
static Network ResolveNetwork(string networkName)
{
    var normalized = networkName.Trim().ToLowerInvariant();

    return normalized switch
    {
        "main" or "mainnet" => Network.Main,
        "test" or "testnet" => Network.TestNet,
        "reg" or "regtest" => Network.RegTest,
        "signet" => ResolveNamedNetwork("signet"),
        "testnet4" or "test4" => Network.TestNet4,
        _ => throw new InvalidOperationException(
            $"Unsupported network '{networkName}'. Supported values: mainnet, testnet, testnet4, signet, regtest.")
    };
}

/// <summary>
/// Resolves a named network using NBitcoin's network registry.
/// </summary>
/// <param name="networkName">The network name to resolve (e.g., "signet").</param>
/// <returns>The corresponding NBitcoin Network instance.</returns>
/// <exception cref="InvalidOperationException">Thrown when NBitcoin cannot resolve the network name.</exception>
static Network ResolveNamedNetwork(string networkName)
{
    return Network.GetNetwork(networkName)
           ?? throw new InvalidOperationException($"NBitcoin cannot resolve network '{networkName}'.");
}

/// <summary>
/// Parses a log level string to a Serilog LogEventLevel.
/// </summary>
static LogEventLevel ParseLevel(string level)
{
    return Enum.TryParse<LogEventLevel>(level, true, out var parsed)
        ? parsed
        : LogEventLevel.Information;
}