using Electre.Logging;

namespace Electre;

/// <summary>
///     Application configuration settings for the Electrum protocol server.
///     This sealed class contains all configurable parameters for the server, including
///     database paths, network settings, Bitcoin RPC credentials, rate limiting, and
///     performance tuning options. Configuration is typically loaded from appsettings.json
///     via dependency injection.
/// </summary>
public sealed class Config
{
    /// <summary>
    ///     Path to the RocksDB database file for storing blockchain index data.
    /// </summary>
    public string DatabasePath { get; set; } = "./data/electre.db";

    /// <summary>
    ///     TCP port for unencrypted Electrum protocol connections (default: 50001).
    /// </summary>
    public int ServerPort { get; set; } = 50001;

    /// <summary>
    ///     TCP port for SSL/TLS-encrypted Electrum protocol connections (default: 50002).
    /// </summary>
    public int SslPort { get; set; } = 50002;

    /// <summary>
    ///     Path to the PKCS#12 (.pfx/.p12) certificate file for SSL/TLS connections.
    ///     If null or empty, SSL is disabled.
    /// </summary>
    public string? SslCertPath { get; set; }

    /// <summary>
    ///     Password for the SSL certificate file, if the certificate is encrypted.
    /// </summary>
    public string? SslCertPassword { get; set; }

    /// <summary>
    ///     URL of the Bitcoin Core RPC endpoint (e.g., http://127.0.0.1:8332).
    /// </summary>
    public string BitcoinRpcUrl { get; set; } = "http://127.0.0.1:8332";

    /// <summary>
    ///     Username for Bitcoin Core RPC authentication.
    /// </summary>
    public string BitcoinRpcUser { get; set; } = "";

    /// <summary>
    ///     Password for Bitcoin Core RPC authentication.
    /// </summary>
    public string BitcoinRpcPassword { get; set; } = "";

    /// <summary>
    ///     Bitcoin network to connect to: mainnet, testnet, testnet4, signet, or regtest.
    /// </summary>
    public string Network { get; set; } = "mainnet";

    /// <summary>
    ///     Server banner message sent to clients during protocol negotiation.
    /// </summary>
    public string ServerBanner { get; set; } = "Electre - .NET Electrum Server";

    /// <summary>
    ///     Donation address displayed to clients (optional).
    /// </summary>
    public string DonationAddress { get; set; } = "";

    /// <summary>
    ///     Maximum number of concurrent client connections allowed (default: 1000).
    /// </summary>
    public int MaxClients { get; set; } = 1000;

    /// <summary>
    ///     Number of blocks to process in each synchronization batch (default: 100).
    /// </summary>
    public int SyncBatchSize { get; set; } = 100;

    /// <summary>
    ///     Disables the RocksDB Write-Ahead Log (WAL) during Initial Block Download (IBD) for faster syncing.
    ///     If true, a crash during IBD will result in data loss and syncing will restart from zero.
    ///     If false (default), syncing is crash-safe but slightly slower.
    /// </summary>
    public bool DisableWalDuringIbd { get; set; } = false;

    /// <summary>
    ///     Maximum depth of blockchain reorganizations to handle (default: 100 blocks).
    /// </summary>
    public int MaxReorgDepth { get; set; } = 100;

    /// <summary>
    ///     Timeout in seconds for graceful shutdown before forcing termination (default: 30).
    /// </summary>
    public int ShutdownTimeoutSeconds { get; set; } = 30;

    /// <summary>
    ///     Maximum number of retry attempts for Bitcoin RPC calls (default: 3).
    /// </summary>
    public int RpcMaxRetries { get; set; } = 3;

    /// <summary>
    ///     Delay in milliseconds between Bitcoin RPC retry attempts (default: 1000).
    /// </summary>
    public int RpcRetryDelayMs { get; set; } = 1000;

    /// <summary>
    ///     Timeout in seconds for individual Bitcoin RPC requests (default: 300).
    /// </summary>
    public int RpcTimeoutSeconds { get; set; } = 300;

    /// <summary>
    ///     ZMQ URL for subscribing to Bitcoin Core block hash notifications (e.g., tcp://127.0.0.1:28332).
    ///     If null or empty, ZMQ notifications are disabled.
    /// </summary>
    public string? ZmqBlockHashUrl { get; set; }

    /// <summary>
    ///     Enable rate limiting for client requests (default: false).
    /// </summary>
    public bool EnableRateLimit { get; set; } = false;

    /// <summary>
    ///     Maximum number of requests per second allowed per client (default: 100).
    /// </summary>
    public int RateLimitRequestsPerSecond { get; set; } = 100;

    /// <summary>
    ///     Burst size for rate limiting, allowing temporary spikes above the per-second limit (default: 200).
    /// </summary>
    public int RateLimitBurstSize { get; set; } = 200;

    /// <summary>
    ///     Maximum number of concurrent connections allowed from a single IP address (default: 10).
    /// </summary>
    public int MaxConnectionsPerIp { get; set; } = 10;

    /// <summary>
    ///     Maximum buffered request line size in bytes before disconnecting the client (default: 1 MiB).
    /// </summary>
    public int MaxRequestLineBytes { get; set; } = 1024 * 1024;

    /// <summary>
    ///     RocksDB block cache size in megabytes. Set to 0 for automatic sizing (25% of available RAM).
    /// </summary>
    public double? DbMem { get; set; }

    /// <summary>
    ///     Maximum number of write buffers in RocksDB before flushing to disk (default: 2).
    /// </summary>
    public int DbMaxWriteBufferNumber { get; set; } = 2;

    /// <summary>
    ///     Maximum number of open file handles for RocksDB (default: 256).
    /// </summary>
    public int DbMaxOpenFiles { get; set; } = 256;

    /// <summary>
    ///     Number of background threads for RocksDB compaction. Set to 0 for automatic (CPU core count).
    /// </summary>
    public int DbParallelism { get; set; } = 0;

    /// <summary>
    ///     Number of concurrent threads for downloading blocks from Bitcoin Core (default: 8).
    /// </summary>
    public int DownloadConcurrency { get; set; } = 8;

    /// <summary>
    ///     Maximum number of blocks queued for download before applying backpressure (default: 200).
    /// </summary>
    public int DownloadQueueSize { get; set; } = 200;

    /// <summary>
    ///     Maximum number of UTXO entries to cache in memory (default: 10,000,000).
    /// </summary>
    public int UtxoCacheMaxEntries { get; set; } = 10_000_000;

    /// <summary>
    ///     Number of UTXO cache entries to flush to disk when threshold is reached (default: 1,000,000).
    /// </summary>
    public int UtxoCacheFlushThreshold { get; set; } = 1_000_000;

    /// <summary>
    ///     Logging configuration settings.
    /// </summary>
    public LoggingConfig Logging { get; set; } = new();

    /// <summary>
    ///     TCP port for Prometheus metrics endpoint (default: 9090).
    /// </summary>
    public int MetricsPort { get; set; } = 9090;

    /// <summary>
    ///     Enable Prometheus metrics collection and HTTP endpoint (default: true).
    /// </summary>
    public bool MetricsEnabled { get; set; } = true;

    /// <summary>
    ///     Maximum number of requests allowed in a single JSON-RPC batch (default: 345).
    ///     Clients sending batches larger than this will receive an error response.
    /// </summary>
    public int MaxBatchSize { get; set; } = 345;

    /// <summary>
    ///     Maximum number of history items allowed per scripthash query (default: 125,000).
    ///     Scripthashes exceeding this limit will return an error instead of exhausting server resources.
    /// </summary>
    public int MaxHistoryItems { get; set; } = 125_000;

    /// <summary>
    ///     Maximum total number of scripthash subscriptions across all clients (default: 10,000,000).
    /// </summary>
    public int MaxSubsGlobally { get; set; } = 10_000_000;

    /// <summary>
    ///     Maximum number of scripthash subscriptions allowed per IP address (default: 50,000).
    /// </summary>
    public int MaxSubsPerIp { get; set; } = 50_000;

    /// <summary>
    ///     Number of TxNum→TxHash entries to cache in memory (default: 3,000,000 ≈ 128 MB).
    ///     TxNum→TxHash mappings are immutable once written, so no cache invalidation is needed.
    ///     Set to 0 to disable caching.
    /// </summary>
    public int TxHashCacheSize { get; set; } = 3_000_000;

    /// <summary>
    ///     Maximum number of concurrent RPC proxy calls to Bitcoin Core across all clients (default: 20).
    /// </summary>
    public int MaxConcurrentRpcProxy { get; set; } = 20;

    /// <summary>
    ///     Maximum number of concurrent RPC proxy calls allowed per IP address (default: 5).
    /// </summary>
    public int RpcProxyPerIpBurst { get; set; } = 5;

    /// <summary>
    ///     TCP port for WebSocket (WS) connections (default: 50003).
    /// </summary>
    public int WsPort { get; set; } = 50003;

    /// <summary>
    ///     TCP port for secure WebSocket (WSS) connections (default: 50004).
    /// </summary>
    public int WssPort { get; set; } = 50004;

    /// <summary>
    ///     Enable WebSocket server (default: false).
    /// </summary>
    public bool WsEnabled { get; set; } = false;

    /// <summary>
    ///     Delay in milliseconds between polling for new blocks when synced (default: 5000).
    /// </summary>
    public int LivePollDelayMs { get; set; } = 5000;

    /// <summary>
    ///     Timeout in milliseconds to wait for a new block notification before polling (default: 60000).
    /// </summary>
    public int WaitForNewBlockTimeoutMs { get; set; } = 60000;

    /// <summary>
    ///     Number of blocks behind chain tip to trigger catch-up mode (default: 1000).
    /// </summary>
    public int CatchupThresholdBlocks { get; set; } = 1000;

    /// <summary>
    ///     Delay in milliseconds between sync retry attempts (default: 5000).
    /// </summary>
    public int SyncRetryDelayMs { get; set; } = 5000;

    /// <summary>
    ///     Interval in milliseconds for polling the mempool (default: 2000).
    /// </summary>
    public int MempoolPollIntervalMs { get; set; } = 2000;

    /// <summary>
    ///     Indicates whether SSL/TLS is enabled based on the presence of a certificate path.
    /// </summary>
    public bool SslEnabled => !string.IsNullOrWhiteSpace(SslCertPath);
}