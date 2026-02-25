using Prometheus;

namespace Electre.Metrics;

/// <summary>
///     Static Prometheus metric definitions for Electre server monitoring.
/// </summary>
/// <remarks>
///     Provides counters, gauges, and histograms for tracking blockchain synchronization,
///     RPC calls, client connections, and subscription metrics.
/// </remarks>
public static class AppMetrics
{
    /// <summary>
    ///     Counter for total number of blocks indexed.
    /// </summary>
    private static readonly Counter BlocksIndexed = Prometheus.Metrics
        .CreateCounter("electre_blocks_indexed_total", "Total number of blocks indexed");

    /// <summary>
    ///     Counter for total number of transactions indexed.
    /// </summary>
    private static readonly Counter TransactionsIndexed = Prometheus.Metrics
        .CreateCounter("electre_transactions_indexed_total", "Total number of transactions indexed");

    /// <summary>
    ///     Gauge for current indexed block height.
    /// </summary>
    private static readonly Gauge CurrentHeight = Prometheus.Metrics
        .CreateGauge("electre_current_height", "Current indexed block height");

    /// <summary>
    ///     Gauge for Bitcoin chain tip height.
    /// </summary>
    private static readonly Gauge ChainTipHeight = Prometheus.Metrics
        .CreateGauge("electre_chain_tip_height", "Bitcoin chain tip height");

    /// <summary>
    ///     Gauge for number of connected clients.
    /// </summary>
    private static readonly Gauge ConnectedClients = Prometheus.Metrics
        .CreateGauge("electre_connected_clients", "Number of connected clients");

    /// <summary>
    ///     Gauge for number of transactions in tracked mempool.
    /// </summary>
    private static readonly Gauge MempoolSize = Prometheus.Metrics
        .CreateGauge("electre_mempool_size", "Number of transactions in tracked mempool");

    /// <summary>
    ///     Counter for total RPC calls to Bitcoin Core, labeled by method.
    /// </summary>
    private static readonly Counter RpcCallsTotal = Prometheus.Metrics
        .CreateCounter("electre_rpc_calls_total", "Total RPC calls to Bitcoin Core", new CounterConfiguration
        {
            LabelNames = ["method"]
        });

    /// <summary>
    ///     Histogram for RPC call duration in seconds, labeled by method.
    /// </summary>
    private static readonly Histogram RpcCallDuration = Prometheus.Metrics
        .CreateHistogram("electre_rpc_call_duration_seconds", "RPC call duration", new HistogramConfiguration
        {
            LabelNames = ["method"],
            Buckets = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
        });

    /// <summary>
    ///     Counter for total client requests, labeled by method.
    /// </summary>
    private static readonly Counter ClientRequestsTotal = Prometheus.Metrics
        .CreateCounter("electre_client_requests_total", "Total client requests", new CounterConfiguration
        {
            LabelNames = ["method"]
        });

    /// <summary>
    ///     Counter for total rate-limited requests.
    /// </summary>
    private static readonly Counter RateLimitedRequests = Prometheus.Metrics
        .CreateCounter("electre_rate_limited_requests_total", "Total rate limited requests");

    /// <summary>
    ///     Gauge for number of scripthash subscriptions.
    /// </summary>
    private static readonly Gauge SubscriptionsScripthash = Prometheus.Metrics
        .CreateGauge("electre_subscriptions_scripthash", "Number of scripthash subscriptions");

    /// <summary>
    ///     Gauge for number of header subscriptions.
    /// </summary>
    private static readonly Gauge SubscriptionsHeaders = Prometheus.Metrics
        .CreateGauge("electre_subscriptions_headers", "Number of header subscriptions");

    /// <summary>
    ///     Increments the blocks indexed counter by one.
    /// </summary>
    public static void IncrementBlocksIndexed()
    {
        BlocksIndexed.Inc();
    }

    /// <summary>
    ///     Increments the blocks indexed counter by the specified count.
    /// </summary>
    /// <param name="count">The number of blocks to add to the counter.</param>
    public static void IncrementBlocksIndexed(int count)
    {
        BlocksIndexed.Inc(count);
    }

    /// <summary>
    ///     Increments the transactions indexed counter by the specified count.
    /// </summary>
    /// <param name="count">The number of transactions to add to the counter.</param>
    public static void IncrementTransactionsIndexed(int count)
    {
        TransactionsIndexed.Inc(count);
    }

    /// <summary>
    ///     Sets the current indexed block height.
    /// </summary>
    /// <param name="height">The current block height.</param>
    public static void SetCurrentHeight(long height)
    {
        CurrentHeight.Set(height);
    }

    /// <summary>
    ///     Sets the Bitcoin chain tip height.
    /// </summary>
    /// <param name="height">The chain tip height.</param>
    public static void SetChainTipHeight(long height)
    {
        ChainTipHeight.Set(height);
    }

    /// <summary>
    ///     Sets the number of connected clients.
    /// </summary>
    /// <param name="count">The number of connected clients.</param>
    public static void SetConnectedClients(int count)
    {
        ConnectedClients.Set(count);
    }

    /// <summary>
    ///     Increments the connected clients counter by one.
    /// </summary>
    public static void IncrementConnectedClients()
    {
        ConnectedClients.Inc();
    }

    /// <summary>
    ///     Decrements the connected clients counter by one.
    /// </summary>
    public static void DecrementConnectedClients()
    {
        ConnectedClients.Dec();
    }

    /// <summary>
    ///     Sets the number of transactions in the tracked mempool.
    /// </summary>
    /// <param name="count">The mempool transaction count.</param>
    public static void SetMempoolSize(int count)
    {
        MempoolSize.Set(count);
    }

    /// <summary>
    ///     Records an RPC call for the specified method.
    /// </summary>
    /// <param name="method">The RPC method name.</param>
    public static void RecordRpcCall(string method)
    {
        RpcCallsTotal.WithLabels(method).Inc();
    }

    /// <summary>
    ///     Measures the duration of an RPC call for the specified method.
    /// </summary>
    /// <param name="method">The RPC method name.</param>
    /// <returns>A disposable timer that records the duration when disposed.</returns>
    public static IDisposable MeasureRpcCall(string method)
    {
        return RpcCallDuration.WithLabels(method).NewTimer();
    }

    /// <summary>
    ///     Records a client request for the specified method.
    /// </summary>
    /// <param name="method">The client request method name.</param>
    public static void RecordClientRequest(string method)
    {
        ClientRequestsTotal.WithLabels(method).Inc();
    }

    /// <summary>
    ///     Increments the rate-limited requests counter by one.
    /// </summary>
    public static void IncrementRateLimited()
    {
        RateLimitedRequests.Inc();
    }

    /// <summary>
    ///     Sets the number of scripthash subscriptions.
    /// </summary>
    /// <param name="count">The number of active scripthash subscriptions.</param>
    public static void SetScripthashSubscriptions(int count)
    {
        SubscriptionsScripthash.Set(count);
    }

    /// <summary>
    ///     Sets the number of header subscriptions.
    /// </summary>
    /// <param name="count">The number of active header subscriptions.</param>
    public static void SetHeaderSubscriptions(int count)
    {
        SubscriptionsHeaders.Set(count);
    }
}