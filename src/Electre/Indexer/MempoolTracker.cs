using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Electre.Bitcoin;
using Electre.Database;
using Electre.Metrics;
using Electre.Server;
using Microsoft.Extensions.Logging;
using NBitcoin;

namespace Electre.Indexer;

/// <summary>
///     Tracks unconfirmed transactions in the Bitcoin mempool and notifies subscribers of changes.
///     Polls the mempool periodically, maintains a cache of mempool transactions indexed by scripthash,
///     and computes Electrum status hashes for affected scripthashes.
/// </summary>
public sealed class MempoolTracker
{
    private readonly ConcurrentDictionary<string, HashSet<string>> _byScripthash = new();
    private readonly Config _config;
    private readonly Func<byte[], List<(int height, byte[] txHash)>> _getConfirmedHistory;
    private readonly object _lock = new();
    private readonly ILogger _logger;
    private readonly Network _network;
    private readonly LruCache<string, PrevoutInfo> _prevoutCache = new(50_000);

    private readonly RpcClient _rpc;
    private readonly LruCache<string, string?> _statusCache;
    private readonly SubscriptionManager _subscriptions;

    private readonly ConcurrentDictionary<string, MempoolTx> _txs = new();

    private readonly ConcurrentDictionary<string, long> _unconfirmedDeltaByScripthash =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Cached fee histogram, invalidated on each mempool poll cycle.
    /// </summary>
    private volatile List<(double feeRate, long vsize)>? _feeHistogramCache;

    /// <summary>
    ///     Initializes a new instance of the MempoolTracker class.
    /// </summary>
    /// <param name="rpc">Bitcoin Core RPC client for fetching mempool data.</param>
    /// <param name="network">Bitcoin network (mainnet, testnet, etc.).</param>
    /// <param name="subscriptions">Subscription manager for notifying clients of mempool changes.</param>
    /// <param name="getConfirmedHistory">Callback to retrieve confirmed transaction history for a scripthash.</param>
    /// <param name="statusCache">Cache for computed status hashes.</param>
    /// <param name="config">Configuration options.</param>
    /// <param name="loggerFactory">Logger factory for creating loggers.</param>
    public MempoolTracker(
        RpcClient rpc,
        Network network,
        SubscriptionManager subscriptions,
        Func<byte[], List<(int height, byte[] txHash)>> getConfirmedHistory,
        LruCache<string, string?> statusCache,
        Config config,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger("Mempool");
        _rpc = rpc;
        _network = network;
        _subscriptions = subscriptions;
        _getConfirmedHistory = getConfirmedHistory;
        _statusCache = statusCache;
        _config = config;
    }

    /// <summary>
    ///     Gets the number of transactions currently in the mempool.
    /// </summary>
    public int Count => _txs.Count;

    /// <summary>
    ///     Runs the mempool tracker loop, polling for changes and notifying subscribers.
    /// </summary>
    /// <param name="ct">Cancellation token to stop the tracker.</param>
    public async Task RunAsync(CancellationToken ct)
    {
        _logger.LogInformation("Mempool tracker started (poll interval: {Interval}s)",
            _config.MempoolPollIntervalMs / 1000);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await PollMempoolAsync();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mempool poll error");
            }

            await Task.Delay(_config.MempoolPollIntervalMs, ct);
        }
    }

    /// <summary>
    ///     Polls the mempool for changes, adds new transactions, and removes expired ones.
    /// </summary>
    private async Task PollMempoolAsync()
    {
        var currentTxids = await _rpc.GetRawMempoolAsync();
        var currentSet = new HashSet<string>(currentTxids, StringComparer.OrdinalIgnoreCase);
        var affectedScripthashes = new HashSet<string>();

        // Manual loop instead of Where().ToList() to avoid allocations
        var toRemove = new List<string>(_txs.Count);
        foreach (var txid in _txs.Keys)
            if (!currentSet.Contains(txid))
                toRemove.Add(txid);

        foreach (var txid in toRemove)
            if (_txs.TryRemove(txid, out var removed))
            {
                foreach (var sh in removed.AffectedScripthashes)
                {
                    affectedScripthashes.Add(sh);
                    RemoveFromScripthashIndex(sh, txid);
                }

                foreach (var (sh, delta) in removed.BalanceDeltas)
                {
                    affectedScripthashes.Add(sh);
                    ApplyScripthashDelta(sh, -delta);
                }
            }

        // Manual loop instead of Where().ToArray() to avoid allocations
        var newTxids = new List<string>(currentTxids.Length);
        foreach (var txid in currentTxids)
            if (!_txs.ContainsKey(txid))
                newTxids.Add(txid);

        if (newTxids.Count > 0)
        {
            const int batchSize = 50;
            for (var i = 0; i < newTxids.Count; i += batchSize)
            {
                // Use Math.Min to avoid allocating beyond array bounds
                var batchEnd = Math.Min(i + batchSize, newTxids.Count);
                var batchLength = batchEnd - i;
                var batch = new string[batchLength];
                newTxids.CopyTo(i, batch, 0, batchLength);
                await ProcessMempoolBatchAsync(batch, affectedScripthashes);
            }
        }

        AppMetrics.SetMempoolSize(_txs.Count);

        // Invalidate fee histogram cache after mempool changes
        _feeHistogramCache = null;

        foreach (var sh in affectedScripthashes)
        {
            _statusCache.Remove(sh);
            var scripthashBytes = ScriptHashUtil.FromHex(sh);
            var status = GetStatus(scripthashBytes);
            await _subscriptions.NotifyScripthashAsync(sh, status);
        }
    }

    /// <summary>
    ///     Processes a batch of mempool transaction IDs, fetching their data and indexing them.
    /// </summary>
    /// <param name="txids">Array of transaction IDs to process.</param>
    /// <param name="affectedScripthashes">Set to accumulate scripthashes affected by this batch.</param>
    private async Task ProcessMempoolBatchAsync(string[] txids, HashSet<string> affectedScripthashes)
    {
        var batchResults = await _rpc.GetMempoolTxBatchAsync(txids);

        var parsedTxs =
            new List<(string txid, JsonNode entry, Transaction tx, byte[] rawTx, long fee, int vsize, int height)>();
        var neededPrevouts = new Dictionary<string, List<(int txIndex, uint outputIndex)>>();

        for (var i = 0; i < txids.Length; i++)
        {
            var txid = txids[i];
            var (entry, rawHex) = batchResults[i];

            if (entry is null || string.IsNullOrWhiteSpace(rawHex))
                continue;

            try
            {
                var rawTx = Convert.FromHexString(rawHex);
                var tx = Transaction.Parse(rawHex, _network);

                var fee = 0L;
                if (entry["fees"]?["base"] is JsonValue feeValue && feeValue.TryGetValue<double>(out var feeDouble))
                    fee = (long)(feeDouble * 100_000_000);

                var vsize = tx.GetVirtualSize();
                if (entry["vsize"] is JsonValue vsizeValue && vsizeValue.TryGetValue<int>(out var vsizeInt))
                    vsize = vsizeInt;

                var hasUnconfirmedInputs = entry["depends"]?.AsArray()?.Count > 0;
                var height = hasUnconfirmedInputs ? -1 : 0;

                parsedTxs.Add((txid, entry, tx, rawTx, fee, vsize, height));

                foreach (var input in tx.Inputs)
                {
                    if (input.PrevOut.Hash == uint256.Zero)
                        continue;

                    var prevTxid = input.PrevOut.Hash.ToString();
                    var cacheKey = $"{prevTxid}:{input.PrevOut.N}";

                    if (!_prevoutCache.TryGet(cacheKey, out _))
                    {
                        if (!neededPrevouts.TryGetValue(prevTxid, out var list))
                        {
                            list = [];
                            neededPrevouts[prevTxid] = list;
                        }

                        list.Add((parsedTxs.Count - 1, input.PrevOut.N));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to parse mempool tx {Txid}", txid);
            }
        }

        if (neededPrevouts.Count > 0)
        {
            var prevTxids = neededPrevouts.Keys.ToArray();
            var prevTxResults = await _rpc.GetRawTransactionsVerboseAsync(prevTxids);

            for (var i = 0; i < prevTxids.Length; i++)
            {
                var prevTxid = prevTxids[i];
                var prevTxData = prevTxResults[i];
                var vout = prevTxData?["vout"]?.AsArray();

                if (vout is null)
                    continue;

                foreach (var (txIndex, outputIndex) in neededPrevouts[prevTxid])
                    if (outputIndex < vout.Count)
                    {
                        var outputNode = vout[(int)outputIndex];
                        var hexNode = outputNode?["scriptPubKey"]?["hex"];
                        var valueNode = outputNode?["value"];
                        if (hexNode is JsonValue hexVal &&
                            hexVal.TryGetValue<string>(out var scriptHex) &&
                            TryParseSatoshis(valueNode, out var valueSats))
                        {
                            var script = Convert.FromHexString(scriptHex);
                            var cacheKey = $"{prevTxid}:{outputIndex}";
                            _prevoutCache.Set(cacheKey, new PrevoutInfo(script, valueSats));
                        }
                    }
            }
        }

        foreach (var (txid, entry, tx, rawTx, fee, vsize, height) in parsedTxs)
            try
            {
                var scripthashes = new HashSet<string>();
                var balanceDeltas = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                foreach (var output in tx.Outputs)
                {
                    var script = output.ScriptPubKey.ToBytes();
                    var scripthash = ScriptHashUtil.ComputeScriptHash(script);
                    var shHex = ScriptHashUtil.ToHex(scripthash);
                    scripthashes.Add(shHex);
                    AddDelta(balanceDeltas, shHex, output.Value.Satoshi);
                }

                foreach (var input in tx.Inputs)
                {
                    if (input.PrevOut.Hash == uint256.Zero)
                        continue;

                    var cacheKey = $"{input.PrevOut.Hash}:{input.PrevOut.N}";
                    if (_prevoutCache.TryGet(cacheKey, out var prevout))
                    {
                        var shHex = ScriptHashUtil.ToHex(ScriptHashUtil.ComputeScriptHash(prevout.Script));
                        scripthashes.Add(shHex);
                        AddDelta(balanceDeltas, shHex, -prevout.ValueSats);
                    }
                }

                var txidBytes = Convert.FromHexString(txid);
                Array.Reverse(txidBytes);

                var mempoolTx = new MempoolTx(txidBytes, rawTx, fee, vsize, height, scripthashes, balanceDeltas);

                if (_txs.TryAdd(txid, mempoolTx))
                {
                    foreach (var sh in scripthashes)
                    {
                        affectedScripthashes.Add(sh);
                        AddToScripthashIndex(sh, txid);
                    }

                    foreach (var (sh, delta) in balanceDeltas)
                    {
                        affectedScripthashes.Add(sh);
                        ApplyScripthashDelta(sh, delta);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to process mempool tx {Txid}", txid);
            }
    }

    /// <summary>
    ///     Fetches a single mempool transaction and returns it as a MempoolTx object.
    /// </summary>
    /// <param name="txid">Transaction ID to fetch.</param>
    /// <returns>MempoolTx object if found, null otherwise.</returns>
    private async Task<MempoolTx?> FetchMempoolTxAsync(string txid)
    {
        var entry = await _rpc.GetMempoolEntryAsync(txid);
        if (entry is null)
            return null;

        var rawHex = await _rpc.GetRawTransactionHexAsync(txid);
        if (string.IsNullOrWhiteSpace(rawHex))
            return null;

        var rawTx = Convert.FromHexString(rawHex);
        var tx = Transaction.Parse(rawHex, _network);

        var fee = 0L;
        if (entry["fees"]?["base"] is JsonValue feeValue && feeValue.TryGetValue<double>(out var feeDouble))
            fee = (long)(feeDouble * 100_000_000);

        var vsize = tx.GetVirtualSize();
        if (entry["vsize"] is JsonValue vsizeValue && vsizeValue.TryGetValue<int>(out var vsizeInt))
            vsize = vsizeInt;

        var hasUnconfirmedInputs = entry["depends"]?.AsArray()?.Count > 0;
        var height = hasUnconfirmedInputs ? -1 : 0;

        var scripthashes = new HashSet<string>();
        var balanceDeltas = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        var neededPrevouts = new List<(string prevTxid, uint outputIndex)>();

        foreach (var output in tx.Outputs)
        {
            var script = output.ScriptPubKey.ToBytes();
            var scripthash = ScriptHashUtil.ComputeScriptHash(script);
            var shHex = ScriptHashUtil.ToHex(scripthash);
            scripthashes.Add(shHex);
            AddDelta(balanceDeltas, shHex, output.Value.Satoshi);
        }

        foreach (var input in tx.Inputs)
        {
            if (input.PrevOut.Hash == uint256.Zero)
                continue;

            var prevTxid = input.PrevOut.Hash.ToString();
            var cacheKey = $"{prevTxid}:{input.PrevOut.N}";

            if (_prevoutCache.TryGet(cacheKey, out var cachedPrevout))
            {
                var shHex = ScriptHashUtil.ToHex(ScriptHashUtil.ComputeScriptHash(cachedPrevout.Script));
                scripthashes.Add(shHex);
                AddDelta(balanceDeltas, shHex, -cachedPrevout.ValueSats);
            }
            else
            {
                neededPrevouts.Add((prevTxid, input.PrevOut.N));
            }
        }

        if (neededPrevouts.Count > 0)
        {
            // Use HashSet to deduplicate instead of Select().Distinct().ToArray()
            var uniquePrevTxidsSet = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (prevTxid, _) in neededPrevouts)
                uniquePrevTxidsSet.Add(prevTxid);

            var uniquePrevTxids = uniquePrevTxidsSet.ToArray();
            var prevTxResults = await _rpc.GetRawTransactionsVerboseAsync(uniquePrevTxids);
            var prevTxMap = new Dictionary<string, JsonNode?>();

            for (var i = 0; i < uniquePrevTxids.Length; i++)
                prevTxMap[uniquePrevTxids[i]] = prevTxResults[i];

            foreach (var (prevTxid, outputIndex) in neededPrevouts)
            {
                if (!prevTxMap.TryGetValue(prevTxid, out var prevTxData))
                    continue;

                var vout = prevTxData?["vout"]?.AsArray();
                if (vout is not null && outputIndex < vout.Count)
                {
                    var outputNode = vout[(int)outputIndex];
                    var hexNode = outputNode?["scriptPubKey"]?["hex"];
                    var valueNode = outputNode?["value"];
                    if (hexNode is JsonValue hexVal &&
                        hexVal.TryGetValue<string>(out var scriptHex) &&
                        TryParseSatoshis(valueNode, out var valueSats))
                    {
                        var script = Convert.FromHexString(scriptHex);
                        var cacheKey = $"{prevTxid}:{outputIndex}";
                        _prevoutCache.Set(cacheKey, new PrevoutInfo(script, valueSats));

                        var shHex = ScriptHashUtil.ToHex(ScriptHashUtil.ComputeScriptHash(script));
                        scripthashes.Add(shHex);
                        AddDelta(balanceDeltas, shHex, -valueSats);
                    }
                }
            }
        }

        var txidBytes = Convert.FromHexString(txid);
        Array.Reverse(txidBytes);

        return new MempoolTx(txidBytes, rawTx, fee, vsize, height, scripthashes, balanceDeltas);
    }

    /// <summary>
    ///     Adds a transaction ID to the scripthash index.
    /// </summary>
    /// <param name="scripthash">Scripthash hex string.</param>
    /// <param name="txid">Transaction ID to add.</param>
    private void AddToScripthashIndex(string scripthash, string txid)
    {
        var set = _byScripthash.GetOrAdd(scripthash, _ => []);
        lock (set)
        {
            set.Add(txid);
        }
    }

    /// <summary>
    ///     Removes a transaction ID from the scripthash index.
    /// </summary>
    /// <param name="scripthash">Scripthash hex string.</param>
    /// <param name="txid">Transaction ID to remove.</param>
    private void RemoveFromScripthashIndex(string scripthash, string txid)
    {
        if (_byScripthash.TryGetValue(scripthash, out var set))
            lock (set)
            {
                set.Remove(txid);
            }
    }

    /// <summary>
    ///     Gets all mempool transactions for a given scripthash.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>List of MempoolTx objects for the scripthash.</returns>
    public List<MempoolTx> GetByScripthash(byte[] scripthash)
    {
        var sh = ScriptHashUtil.ToHex(scripthash);
        if (!_byScripthash.TryGetValue(sh, out var txids))
            return [];

        List<MempoolTx> result;
        lock (txids)
        {
            // Manual loop instead of Select().Where().Cast().ToList() to avoid allocations
            result = new List<MempoolTx>(txids.Count);
            foreach (var txid in txids)
                if (_txs.TryGetValue(txid, out var tx))
                    result.Add(tx);
        }

        return result;
    }

    /// <summary>
    ///     Gets the unconfirmed balance for a scripthash from mempool transactions.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>Tuple of (deltaConfirmed, unconfirmedTotal) satoshis.</returns>
    public (long deltaConfirmed, long unconfirmedTotal) GetUnconfirmedBalance(byte[] scripthash)
    {
        var sh = ScriptHashUtil.ToHex(scripthash);

        long deltaConfirmed = 0;
        _unconfirmedDeltaByScripthash.TryGetValue(sh, out var unconfirmedTotal);

        return (deltaConfirmed, unconfirmedTotal);
    }

    private void ApplyScripthashDelta(string scripthash, long delta)
    {
        if (delta == 0)
            return;

        var updated = _unconfirmedDeltaByScripthash.AddOrUpdate(
            scripthash,
            delta,
            (_, current) => current + delta);

        if (updated == 0)
            _unconfirmedDeltaByScripthash.TryRemove(scripthash, out _);
    }

    private static void AddDelta(Dictionary<string, long> deltas, string scripthash, long delta)
    {
        if (delta == 0)
            return;

        if (!deltas.TryAdd(scripthash, delta))
            deltas[scripthash] += delta;
    }

    private static bool TryParseSatoshis(JsonNode? valueNode, out long sats)
    {
        sats = 0;
        if (valueNode is not JsonValue value)
            return false;

        if (value.TryGetValue<decimal>(out var btc))
        {
            sats = (long)(btc * 100_000_000m);
            return true;
        }

        if (value.TryGetValue<double>(out var btcDouble))
        {
            sats = (long)(btcDouble * 100_000_000d);
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Gets the mempool transaction history for a scripthash.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>List of (height, txHash, fee) tuples for mempool transactions.</returns>
    public List<(int height, byte[] txHash, long fee)> GetMempoolHistory(byte[] scripthash)
    {
        var txs = GetByScripthash(scripthash);
        return txs
            .Select(tx => (tx.Height, tx.TxId, tx.Fee))
            .OrderBy(x => x.Height == 0 ? 0 : 1)
            .ThenBy(x => TxIdSortKey(x.TxId), StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    ///     Computes the Electrum status hash for a scripthash, combining confirmed and mempool transactions.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>Status hash hex string, or null if no transactions exist.</returns>
    public string? GetStatus(byte[] scripthash)
    {
        var confirmedHistory = _getConfirmedHistory(scripthash);
        var mempoolTxs = GetByScripthash(scripthash);

        if (confirmedHistory.Count == 0 && mempoolTxs.Count == 0)
            return null;

        // Build sorted mempool entries: height=0 first, then by txid hex ascending
        var mempoolEntries = mempoolTxs
            .OrderBy(t => t.Height == 0 ? 0 : 1)
            .ThenBy(t => TxIdSortKey(t.TxId), StringComparer.Ordinal)
            .Select(t => (t.Height, t.TxId))
            .ToList();

        return StatusHash.Compute(confirmedHistory, mempoolEntries);
    }

    private static string TxIdSortKey(byte[] txId)
    {
        var display = txId.ToArray();
        Array.Reverse(display);
        return Convert.ToHexStringLower(display);
    }

    /// <summary>
    ///     Gets a fee rate histogram for mempool transactions.
    ///     Returns a cached result if available; the cache is invalidated on each poll cycle.
    /// </summary>
    /// <returns>List of (feeRate, vsize) tuples representing fee rate bins.</returns>
    public List<(double feeRate, long vsize)> GetFeeHistogram()
    {
        var cached = _feeHistogramCache;
        if (cached is not null)
            return cached;

        var result = ComputeFeeHistogram();
        _feeHistogramCache = result;
        return result;
    }

    private List<(double feeRate, long vsize)> ComputeFeeHistogram()
    {
        // Pre-allocate list and populate with fee rates, filtering in-place
        var txFeeRates = new List<(double feeRate, long vsize)>(_txs.Count);
        foreach (var tx in _txs.Values)
        {
            var feeRate = tx.Vsize > 0 ? (double)tx.Fee / tx.Vsize : 0;
            if (feeRate > 0)
                txFeeRates.Add((feeRate, tx.Vsize));
        }

        if (txFeeRates.Count == 0)
            return [];

        // Sort in-place instead of OrderByDescending().ToList()
        txFeeRates.Sort((a, b) => b.feeRate.CompareTo(a.feeRate));

        const double BinGrowthFactor = 1.1;
        var result = new List<(double feeRate, long vsize)>();
        var binThreshold = txFeeRates[0].feeRate;
        long cumulativeVsize = 0;
        var lastReportedVsize = 0L;

        foreach (var (feeRate, vsize) in txFeeRates)
        {
            cumulativeVsize += vsize;

            while (feeRate < binThreshold / BinGrowthFactor && binThreshold > 1)
            {
                if (cumulativeVsize > lastReportedVsize)
                {
                    result.Add((Math.Round(binThreshold, 2), cumulativeVsize - lastReportedVsize));
                    lastReportedVsize = cumulativeVsize;
                }

                binThreshold /= BinGrowthFactor;
            }
        }

        if (cumulativeVsize > lastReportedVsize)
            result.Add((Math.Round(binThreshold, 2), cumulativeVsize - lastReportedVsize));

        return result;
    }

    /// <summary>
    ///     Gets a mempool transaction by its transaction ID.
    /// </summary>
    /// <param name="txid">Transaction ID hex string.</param>
    /// <returns>MempoolTx object if found, null otherwise.</returns>
    public MempoolTx? GetTransaction(string txid)
    {
        _txs.TryGetValue(txid, out var tx);
        return tx;
    }

    /// <summary>
    ///     Gets the raw transaction bytes for a mempool transaction.
    /// </summary>
    /// <param name="txid">Transaction ID bytes.</param>
    /// <returns>Raw transaction bytes if found, null otherwise.</returns>
    public byte[]? GetRawTransaction(byte[] txid)
    {
        var txidHex = Convert.ToHexStringLower(txid.Reverse().ToArray());
        if (_txs.TryGetValue(txidHex, out var tx))
            return tx.RawTx;
        return null;
    }
}

/// <summary>
///     Represents a transaction in the Bitcoin mempool.
/// </summary>
/// <param name="TxId">Transaction ID bytes (32 bytes, big-endian).</param>
/// <param name="RawTx">Raw transaction bytes.</param>
/// <param name="Fee">Transaction fee in satoshis.</param>
/// <param name="Vsize">Virtual size of the transaction in vbytes.</param>
/// <param name="Height">Block height (0 for unconfirmed, -1 for unconfirmed with unconfirmed inputs).</param>
/// <param name="AffectedScripthashes">Set of scripthashes affected by this transaction.</param>
public sealed record MempoolTx(
    byte[] TxId,
    byte[] RawTx,
    long Fee,
    int Vsize,
    int Height,
    HashSet<string> AffectedScripthashes,
    IReadOnlyDictionary<string, long> BalanceDeltas
);

internal readonly record struct PrevoutInfo(byte[] Script, long ValueSats);