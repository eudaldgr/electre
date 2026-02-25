using System.Diagnostics;
using Electre.Bitcoin;
using Electre.Database;
using Electre.Metrics;
using Electre.Server;
using Electre.Utils;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBitcoin;
using RocksDbSharp;

namespace Electre.Indexer;

/// <summary>
///     Main blockchain synchronization service that orchestrates block download, parsing, validation, and indexing.
///     Runs as a background service, detecting reorgs, managing the mempool, and notifying subscribers of changes.
/// </summary>
public sealed class Syncer : BackgroundService
{
    private readonly BlockValidator _blockValidator;
    private readonly Config _config;
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Network _network;
    private readonly SemaphoreSlim _newBlockSignal = new(0);

    private readonly ReorgManager _reorgManager;
    private readonly RpcClient _rpc;

    private readonly Store _store;
    private readonly SubscriptionManager _subscriptions;
    private readonly ZmqBlockNotifier? _zmqNotifier;

    /// <summary>
    ///     Initializes a new instance of the Syncer class.
    /// </summary>
    /// <param name="store">RocksDB store for blockchain data.</param>
    /// <param name="rpc">Bitcoin Core RPC client.</param>
    /// <param name="config">Configuration options.</param>
    /// <param name="network">Bitcoin network.</param>
    /// <param name="subscriptions">Subscription manager for client notifications.</param>
    /// <param name="loggerFactory">Logger factory.</param>
    /// <param name="reorgManager">Reorg manager for handling blockchain reorgs.</param>
    /// <param name="blockValidator">Block validator for header verification.</param>
    /// <param name="utxoCache">UTXO cache for in-memory UTXO set.</param>
    /// <param name="statusCache">Cache for computed status hashes.</param>
    public Syncer(
        Store store,
        RpcClient rpc,
        IOptions<Config> config,
        Network network,
        SubscriptionManager subscriptions,
        ILoggerFactory loggerFactory,
        ReorgManager reorgManager,
        BlockValidator blockValidator,
        UtxoCache utxoCache,
        LruCache<string, string?> statusCache)
    {
        _logger = loggerFactory.CreateLogger("Syncer");
        _loggerFactory = loggerFactory;
        _store = store;
        _rpc = rpc;
        _config = config.Value;
        _network = network;
        _subscriptions = subscriptions;

        _reorgManager = reorgManager;
        _blockValidator = blockValidator;
        UtxoCache = utxoCache;
        StatusCache = statusCache;

        CurrentHeight = store.GetSyncedHeight();
        CurrentHeader = CurrentHeight >= 0 ? store.GetBlockHeader(CurrentHeight) : null;

        _blockValidator.VerifyGenesisBlock(CurrentHeight);

        Mempool = new MempoolTracker(rpc, network, subscriptions, GetConfirmedHistory, StatusCache, _config,
            loggerFactory);

        // Initialize ZMQ block notifier if configured
        if (!string.IsNullOrWhiteSpace(_config.ZmqBlockHashUrl))
            _zmqNotifier = new ZmqBlockNotifier(_config.ZmqBlockHashUrl, _newBlockSignal, loggerFactory);
    }

    /// <summary>
    ///     Gets the current synced block height.
    /// </summary>
    public int CurrentHeight { get; private set; }

    /// <summary>
    ///     Gets the current block header bytes (80 bytes).
    /// </summary>
    public byte[]? CurrentHeader { get; private set; }

    /// <summary>
    ///     Gets the mempool tracker instance.
    /// </summary>
    public MempoolTracker? Mempool { get; }

    /// <summary>
    ///     Gets the chain tip height from Bitcoin Core.
    /// </summary>
    public int ChainTipHeight { get; private set; }

    /// <summary>
    ///     Gets a value indicating whether the syncer is caught up with the chain tip.
    /// </summary>
    public bool IsSynced => CurrentHeight >= ChainTipHeight && ChainTipHeight > 0;

    /// <summary>
    ///     Gets the UTXO cache instance.
    /// </summary>
    public UtxoCache UtxoCache { get; }

    /// <summary>
    ///     Gets the status hash cache instance.
    /// </summary>
    public LruCache<string, string?> StatusCache { get; }

    /// <summary>
    ///     Gets the confirmed transaction history for a scripthash from the database.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>List of (height, txHash) tuples.</returns>
    private List<(int height, byte[] txHash)> GetConfirmedHistory(byte[] scripthash)
    {
        var history = _store.GetScripthashHistory(scripthash);
        return history;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await RecoverDirtyStateIfNeededAsync(ct);

        _logger.LogInformation("Starting sync from height {Height}", CurrentHeight + 1);
        var atTip = false;

        _ = Mempool?.RunAsync(ct).ContinueWith(t =>
            _logger.LogError(t.Exception, "MempoolTracker crashed"), TaskContinuationOptions.OnlyOnFaulted);
        _ = _zmqNotifier?.RunAsync(ct).ContinueWith(t =>
            _logger.LogError(t.Exception, "ZmqBlockNotifier crashed"), TaskContinuationOptions.OnlyOnFaulted);
        _ = WaitForNewBlockLoopAsync(ct).ContinueWith(t =>
            _logger.LogError(t.Exception, "WaitForNewBlock loop crashed"), TaskContinuationOptions.OnlyOnFaulted);

        while (!ct.IsCancellationRequested)
            try
            {
                await RecoverDirtyStateIfNeededAsync(ct);

                var chainHeight = await _rpc.GetBlockCountAsync();
                ChainTipHeight = chainHeight;

                if (CurrentHeight < chainHeight)
                {
                    atTip = false;
                    var reorgDepth = await _reorgManager.DetectReorgAsync(CurrentHeight, CurrentHeader);
                    if (reorgDepth > 0)
                    {
                        _logger.LogWarning("Reorg detected! Depth: {Depth}", reorgDepth);
                        CurrentHeight = await _reorgManager.HandleReorgAsync(CurrentHeight, reorgDepth);
                        CurrentHeader = CurrentHeight >= 0 ? _store.GetBlockHeader(CurrentHeight) : null;
                    }

                    await SyncBlocksAsync(CurrentHeight + 1, chainHeight, ct);
                }
                else
                {
                    if (!atTip)
                    {
                        _logger.LogInformation(
                            "At chain tip ({Height}). Waiting for new blocks and polling every {Seconds}s.",
                            CurrentHeight,
                            _config.LivePollDelayMs / 1000);
                        atTip = true;
                    }

                    await _newBlockSignal.WaitAsync(TimeSpan.FromMilliseconds(_config.LivePollDelayMs), ct);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sync error");
                await Task.Delay(_config.SyncRetryDelayMs, ct);
            }

        // Graceful shutdown: disable IBD mode and flush pending data
        try
        {
            _store.SetIbdMode(false);
            UtxoCache.SetIbdMode(false);
            await UtxoCache.FlushToDbAsync();
            _store.FlushMemTables();
            _logger.LogInformation("Graceful shutdown complete, all data flushed to disk");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during graceful shutdown cleanup");
        }
    }

    /// <summary>
    ///     Gets the block hash from a block header.
    /// </summary>
    /// <param name="header">Block header bytes (80 bytes).</param>
    /// <returns>Block hash as hex string.</returns>
    private static string GetBlockHashFromHeader(byte[] header)
    {
        return BlockValidator.GetBlockHashFromHeader(header);
    }

    /// <summary>
    ///     Waits for new block notifications from Bitcoin Core or ZMQ.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    private async Task WaitForNewBlockLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
            try
            {
                var currentTip = CurrentHeader is not null ? GetBlockHashFromHeader(CurrentHeader) : null;
                var result = await _rpc.WaitForNewBlockAsync(currentTip, _config.WaitForNewBlockTimeoutMs, ct);
                if (result is not null)
                    _newBlockSignal.Release();
                else
                    await Task.Delay(_config.SyncRetryDelayMs, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "waitfornewblock error, retrying in {Delay}ms", _config.SyncRetryDelayMs);
                await Task.Delay(_config.SyncRetryDelayMs, ct);
            }
    }

    /// <summary>
    ///     Synchronizes blocks from a given height range, downloading, parsing, validating, and indexing them.
    /// </summary>
    /// <param name="fromHeight">Starting block height (inclusive).</param>
    /// <param name="toHeight">Ending block height (inclusive).</param>
    /// <param name="ct">Cancellation token.</param>
    private async Task SyncBlocksAsync(int fromHeight, int toHeight, CancellationToken ct)
    {
        var writeBatchSize = _config.SyncBatchSize;
        var downloadConcurrency = _config.DownloadConcurrency;
        var maxQueueSize = _config.DownloadQueueSize;

        var catchupMode = toHeight - fromHeight > _config.CatchupThresholdBlocks;
        if (catchupMode)
        {
            _logger.LogInformation(
                "Catch-up mode: {BlocksBehind} blocks behind, notifications disabled, WAL {WalStatus}",
                toHeight - fromHeight, _config.DisableWalDuringIbd ? "disabled" : "enabled");
            _store.SetIbdMode(_config.DisableWalDuringIbd);
            UtxoCache.SetIbdMode(true);
        }
        else
        {
            _store.SetIbdMode(false);
            UtxoCache.SetIbdMode(false);
        }

        await using var downloader = new BlockDownloader(
            _config.BitcoinRpcUrl,
            _config.BitcoinRpcUser,
            _config.BitcoinRpcPassword,
            _network,
            _loggerFactory,
            _config.RpcMaxRetries,
            _config.RpcRetryDelayMs,
            _config.RpcTimeoutSeconds,
            downloadConcurrency,
            maxQueueSize);

        downloader.Start(fromHeight, toHeight);

        var batch = _store.CreateWriteBatch();
        try
        {
            // Use byte[] for affected scripthashes to avoid hex string allocations in the hot loop
            var affectedScripthashes = new HashSet<byte[]>(ByteArrayComparer.Instance);
            var blocksInBatch = 0;
            var lastLoggedHeight = fromHeight - 1;
            var previousHeader = CurrentHeader;
            var lastFlushTime = Stopwatch.GetTimestamp();
            var flushIntervalTicks = 120L * Stopwatch.Frequency;

            while (!ct.IsCancellationRequested)
            {
                var block = await downloader.WaitForNextBlockAsync(ct);
                if (block is null)
                    break;

                _blockValidator.VerifyBlockHeader(block.Height, block.Header, previousHeader);
                IndexBlock(block, batch, affectedScripthashes);
                previousHeader = block.Header;
                blocksInBatch++;

                if (blocksInBatch >= writeBatchSize || block.Height == toHeight)
                {
                    _store.BeginMutation(DirtyOperationKind.Sync, CurrentHeight);
                    try
                    {
                        _reorgManager.CleanupOldUndoBlocks(batch, block.Height);
                        _store.SetSyncedHeight(batch, block.Height);
                        _store.Write(batch);
                        _store.TxNumManager.CommitBatch();
                        await UtxoCache.FlushToDbAsync();
                        _store.EndMutation();

                        if (!catchupMode)
                            _store.SyncWal();
                    }
                    catch
                    {
                        _store.TxNumManager.CommitBatch();
                        throw;
                    }

                    CurrentHeight = block.Height;
                    CurrentHeader = block.Header;

                    AppMetrics.IncrementBlocksIndexed(blocksInBatch);
                    AppMetrics.SetCurrentHeight(CurrentHeight);
                    AppMetrics.SetChainTipHeight(ChainTipHeight);

                    if (!catchupMode)
                    {
                        if (CurrentHeader is not null)
                            await _subscriptions.NotifyHeadersAsync(CurrentHeight, CurrentHeader);

                        foreach (var scripthash in affectedScripthashes)
                        {
                            var shHex = ScriptHashUtil.ToHex(scripthash);
                            StatusCache.Remove(shHex);

                            var history = _store.GetScripthashHistory(scripthash);
                            var status = StatusHash.Compute(history);
                            StatusCache.Set(shHex, status);
                            await _subscriptions.NotifyScripthashAsync(shHex, status);
                        }
                    }
                    else
                    {
                        foreach (var scripthash in affectedScripthashes)
                        {
                            var shHex = ScriptHashUtil.ToHex(scripthash);
                            StatusCache.Remove(shHex);
                        }
                    }

                    // Periodic flush during IBD to persist data that would otherwise
                    // only live in memtables (if WAL is disabled during IBD)
                    if (catchupMode && Stopwatch.GetTimestamp() - lastFlushTime > flushIntervalTicks)
                    {
                        _store.FlushMemTables();
                        lastFlushTime = Stopwatch.GetTimestamp();
                    }

                    if (block.Height - lastLoggedHeight >= 1000 || block.Height == toHeight)
                    {
                        var percent = toHeight > 0 ? block.Height * 100 / toHeight : 100;
                        _logger.LogInformation("Synced to height {Height} ({Percent}%) [queue: {QueueCount}]",
                            block.Height, percent, downloader.QueuedCount);
                        lastLoggedHeight = block.Height;
                    }

                    batch.Dispose();
                    batch = _store.CreateWriteBatch();
                    affectedScripthashes.Clear();
                    blocksInBatch = 0;
                }
            }
        }
        finally
        {
            batch.Dispose();
            _store.TxNumManager.CommitBatch();
        }

        if (catchupMode)
        {
            _store.SetIbdMode(false);
            UtxoCache.SetIbdMode(false);
            if (CurrentHeader is not null)
            {
                _logger.LogInformation("Catch-up complete at height {Height}; live notifications resumed, WAL enabled",
                    CurrentHeight);
                await _subscriptions.NotifyHeadersAsync(CurrentHeight, CurrentHeader);
            }
        }
    }

    private async Task RecoverDirtyStateIfNeededAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var dirtyState = _store.GetDirtyState();
        if (!dirtyState.IsDirty)
            return;

        var syncedHeight = _store.GetSyncedHeight();
        var baseHeight = dirtyState.BaseHeight;

        if (baseHeight is null || dirtyState.OperationKind is DirtyOperationKind.None)
            throw new InvalidOperationException(
                "Database state is inconsistent: dirty marker present without recovery metadata. Fatal startup state.");

        var recoverable = dirtyState.OperationKind switch
        {
            DirtyOperationKind.Sync => syncedHeight >= baseHeight.Value,
            DirtyOperationKind.Reorg => syncedHeight <= baseHeight.Value,
            _ => false
        };

        if (!recoverable)
            throw new InvalidOperationException(
                $"Database state is inconsistent and not recoverable (op={dirtyState.OperationKind}, " +
                $"base={baseHeight.Value}, synced={syncedHeight}). Fatal startup state.");

        _logger.LogWarning(
            "Detected recoverable dirty state (op={Operation}, base={Base}, synced={Synced}). Starting recovery.",
            dirtyState.OperationKind,
            baseHeight.Value,
            syncedHeight);

        CurrentHeight = await _reorgManager.RecoverDirtyStateAsync(dirtyState, syncedHeight);
        CurrentHeader = CurrentHeight >= 0 ? _store.GetBlockHeader(CurrentHeight) : null;

        // Clear all in-memory caches after recovery to discard stale data from undone blocks
        StatusCache.Clear();
        await UtxoCache.ClearAllAsync();

        _logger.LogInformation("Recovery complete. Synced height is now {Height}", CurrentHeight);
    }

    /// <summary>
    ///     Indexes a block by storing its header, transactions, UTXOs, and history entries.
    /// </summary>
    /// <param name="block">Block data to index.</param>
    /// <param name="batch">RocksDB write batch for atomic writes.</param>
    /// <param name="affectedScripthashes">Set to accumulate affected scripthashes.</param>
    private void IndexBlock(BlockData block, WriteBatch batch, HashSet<byte[]> affectedScripthashes)
    {
        var height = block.Height;

        _store.PutBlockHeader(batch, height, block.Header);
        _store.PutBlockHash(batch, block.Hash, height);

        var undo = new UndoBlock();
        var txPosition = 0;
        var firstTxNum = 0UL;
        var pendingHistoryEntries = new List<(byte[] scripthash, ulong txNum)>(block.OutputScriptHashes.Length);

        // Track global output index to match flattened OutputScriptHashes array
        var globalOutputIndex = 0;

        foreach (var tx in block.Block.Transactions)
        {
            var txId = tx.GetHash().ToBytes();
            _store.PutTransaction(batch, txId, height, txPosition);

            var txNum = _store.TxNumManager.AssignTxNum(batch, txId, height, txPosition);
            if (txPosition == 0)
                firstTxNum = txNum;

            for (var inputIndex = 0; inputIndex < tx.Inputs.Count; inputIndex++)
            {
                var input = tx.Inputs[inputIndex];
                // Coinbase check: prevOut.Hash == 0
                if (input.PrevOut.Hash == uint256.Zero)
                    continue;

                var prevTxId = input.PrevOut.Hash.ToBytes();
                var prevOutputIndex = (int)input.PrevOut.N;

                var prevTxNum = _store.TxNumManager.GetTxNum(prevTxId);
                if (!prevTxNum.HasValue)
                    throw new InvalidOperationException(
                        $"Block {height} tx {tx.GetHash()} input {inputIndex} references unknown prev tx {input.PrevOut.Hash}:{prevOutputIndex}");

                var utxo = UtxoCache.Get(prevTxNum.Value, prevOutputIndex);
                if (utxo is null)
                    throw new InvalidOperationException(
                        $"Block {height} tx {tx.GetHash()} input {inputIndex} references missing UTXO txnum {prevTxNum.Value}:{prevOutputIndex}");

                var scripthash = utxo.Value.Scripthash;
                var value = utxo.Value.Value;
                var utxoHeight = utxo.Value.Height;

                undo.SpentUtxos.Add(new SpentUtxo(scripthash, prevTxNum.Value, prevOutputIndex, utxoHeight, value));

                UtxoCache.Delete(prevTxNum.Value, prevOutputIndex);
                _store.DeleteScripthashUtxo(batch, scripthash, prevTxNum.Value, prevOutputIndex);
                _store.DeleteUtxo(batch, prevTxNum.Value, prevOutputIndex);
                pendingHistoryEntries.Add((scripthash, txNum));
                undo.HistoryEntries.Add(new HistoryEntry(scripthash, txNum));
                affectedScripthashes.Add(scripthash);
            }

            for (var i = 0; i < tx.Outputs.Count; i++)
            {
                var output = tx.Outputs[i];
                // Retrieve pre-computed scripthash
                var scripthash = block.OutputScriptHashes[globalOutputIndex++];

                UtxoCache.Put(txNum, i, scripthash, output.Value.Satoshi, height);
                _store.PutScripthashUtxo(batch, scripthash, txNum, i, height, output.Value.Satoshi);
                _store.PutUtxo(batch, txNum, i, scripthash, output.Value.Satoshi, height);
                pendingHistoryEntries.Add((scripthash, txNum));

                undo.CreatedUtxos.Add(new CreatedUtxo(scripthash, txNum, i));
                undo.HistoryEntries.Add(new HistoryEntry(scripthash, txNum));
                affectedScripthashes.Add(scripthash);
            }

            txPosition++;
        }

        if (globalOutputIndex != block.OutputScriptHashes.Length)
            throw new InvalidOperationException(
                $"Block {height}: OutputScriptHashes length mismatch. " +
                $"Expected {block.OutputScriptHashes.Length}, processed {globalOutputIndex}");

        _store.PutScripthashHistoryBatch(batch, pendingHistoryEntries);
        _store.TxNumManager.FinishBlock(batch, height, txPosition, firstTxNum);
        _store.PutUndoBlock(batch, height, undo.Serialize());
    }

    /// <summary>
    ///     Gets the confirmed and unconfirmed balance for a scripthash.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>Tuple of (confirmed, unconfirmed) satoshis.</returns>
    public (long confirmed, long unconfirmed) GetBalance(byte[] scripthash)
    {
        var utxos = _store.GetScripthashUtxos(scripthash);
        var confirmed = utxos.Sum(u => u.value);

        var (_, unconfirmedTotal) = Mempool?.GetUnconfirmedBalance(scripthash) ?? (0, 0);
        return (confirmed, unconfirmedTotal);
    }

    /// <summary>
    ///     Gets the transaction history for a scripthash, combining confirmed and mempool transactions.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>List of (height, txHash) tuples.</returns>
    public List<(int height, byte[] txHash)> GetHistory(byte[] scripthash)
    {
        var confirmed = _store.GetScripthashHistory(scripthash, _config.MaxHistoryItems);

        if (Mempool is null)
            return confirmed;

        var mempool = Mempool.GetMempoolHistory(scripthash);
        if (mempool.Count == 0)
            return confirmed;

        var result = new List<(int height, byte[] txHash)>(confirmed.Count + mempool.Count);
        result.AddRange(confirmed);
        result.AddRange(mempool.Select(m => (m.height, m.txHash)));
        return result;
    }

    /// <summary>
    ///     Gets the transaction history for a scripthash with fee information.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>List of (height, txHash, fee) tuples.</returns>
    public List<(int height, byte[] txHash, long fee)> GetHistoryWithFee(byte[] scripthash)
    {
        var confirmed = _store.GetScripthashHistory(scripthash, _config.MaxHistoryItems);
        var result = new List<(int height, byte[] txHash, long fee)>(confirmed.Count);

        foreach (var (height, txHash) in confirmed)
            result.Add((height, txHash, 0));

        if (Mempool is not null)
        {
            var mempool = Mempool.GetMempoolHistory(scripthash);
            result.AddRange(mempool);
        }

        return result;
    }

    /// <summary>
    ///     Gets all unspent transaction outputs (UTXOs) for a scripthash.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>List of (txHash, outputIndex, height, value) tuples.</returns>
    public List<(byte[] txHash, int outputIndex, int height, long value)> GetUtxos(byte[] scripthash)
    {
        return _store.GetScripthashUtxosResolved(scripthash);
    }

    /// <summary>
    ///     Gets the Electrum status hash for a scripthash.
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>Status hash hex string, or null if no transactions exist.</returns>
    public string? GetStatus(byte[] scripthash)
    {
        var scripthashHex = ScriptHashUtil.ToHex(scripthash);
        if (StatusCache.TryGet(scripthashHex, out var cached))
            return cached;

        string? status;
        if (Mempool is not null)
        {
            status = Mempool.GetStatus(scripthash);
        }
        else
        {
            var history = _store.GetScripthashHistory(scripthash, _config.MaxHistoryItems);
            status = StatusHash.Compute(history);
        }

        StatusCache.Set(scripthashHex, status);
        return status;
    }

    /// <summary>
    ///     Disposes the syncer and releases resources.
    /// </summary>
    public override void Dispose()
    {
        _zmqNotifier?.Dispose();
        _newBlockSignal.Dispose();
        base.Dispose();
    }
}