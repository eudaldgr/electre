using Electre.Bitcoin;
using Electre.Database;
using Electre.Server;
using Electre.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RocksDbSharp;

namespace Electre.Indexer;

/// <summary>
///     Manages blockchain reorganizations (reorgs) by detecting when the chain tip changes
///     and undoing blocks to restore consistency with the new chain.
/// </summary>
public sealed class ReorgManager
{
    private readonly ILogger<ReorgManager> _logger;
    private readonly int _maxReorgDepth;
    private readonly RpcClient _rpc;
    private readonly LruCache<string, string?> _statusCache;
    private readonly Store _store;
    private readonly SubscriptionManager _subscriptions;
    private readonly UtxoCache _utxoCache;

    /// <summary>
    ///     Initializes a new instance of the ReorgManager class.
    /// </summary>
    /// <param name="store">RocksDB store for blockchain data.</param>
    /// <param name="rpc">Bitcoin Core RPC client.</param>
    /// <param name="utxoCache">UTXO cache for in-memory UTXO set.</param>
    /// <param name="subscriptions">Subscription manager for client notifications.</param>
    /// <param name="statusCache">Cache for computed status hashes.</param>
    /// <param name="config">Configuration options.</param>
    /// <param name="logger">Logger instance.</param>
    public ReorgManager(
        Store store,
        RpcClient rpc,
        UtxoCache utxoCache,
        SubscriptionManager subscriptions,
        LruCache<string, string?> statusCache,
        IOptions<Config> config,
        ILogger<ReorgManager> logger)
    {
        _store = store;
        _rpc = rpc;
        _utxoCache = utxoCache;
        _subscriptions = subscriptions;
        _statusCache = statusCache;
        _logger = logger;
        _maxReorgDepth = config.Value.MaxReorgDepth;
    }

    /// <summary>
    ///     Detects if a blockchain reorganization has occurred by comparing block hashes.
    /// </summary>
    /// <param name="currentHeight">Current synced block height.</param>
    /// <param name="currentHeader">Current block header bytes.</param>
    /// <returns>Reorg depth (number of blocks to undo), or 0 if no reorg detected.</returns>
    public async Task<int> DetectReorgAsync(int currentHeight, byte[]? currentHeader)
    {
        if (currentHeight < 0 || currentHeader is null)
            return 0;

        var chainHash = await _rpc.GetBlockHashAsync(currentHeight);
        var ourHash = BlockValidator.GetBlockHashFromHeader(currentHeader);

        if (chainHash.Equals(ourHash, StringComparison.OrdinalIgnoreCase))
            return 0;

        for (var depth = 1; depth <= Math.Min(_maxReorgDepth, currentHeight); depth++)
        {
            var checkHeight = currentHeight - depth;
            var checkHeader = _store.GetBlockHeader(checkHeight);
            if (checkHeader is null) break;

            var checkOurHash = BlockValidator.GetBlockHashFromHeader(checkHeader);
            var checkChainHash = await _rpc.GetBlockHashAsync(checkHeight);

            if (checkChainHash.Equals(checkOurHash, StringComparison.OrdinalIgnoreCase))
                return depth;
        }

        throw new InvalidOperationException(
            $"Reorg deeper than {_maxReorgDepth} blocks. Manual intervention required.");
    }

    /// <summary>
    ///     Handles a blockchain reorganization by undoing blocks and restoring the correct chain state.
    /// </summary>
    /// <param name="currentHeight">Current synced block height.</param>
    /// <param name="depth">Number of blocks to undo.</param>
    /// <returns>New synced height after reorg handling.</returns>
    public async Task<int> HandleReorgAsync(int currentHeight, int depth)
    {
        var targetHeight = currentHeight - depth;
        var affectedScripthashes = new HashSet<string>();

        _store.BeginMutation(DirtyOperationKind.Reorg, currentHeight);
        await UndoToHeightAsync(currentHeight, targetHeight, affectedScripthashes);
        _store.EndMutation();

        var newHeight = targetHeight;
        var newHeader = newHeight >= 0 ? _store.GetBlockHeader(newHeight) : null;

        foreach (var sh in affectedScripthashes)
        {
            _statusCache.Remove(sh);
            var scripthashBytes = ScriptHashUtil.FromHex(sh);
            var history = _store.GetScripthashHistory(scripthashBytes);
            var status = StatusHash.Compute(history);
            _statusCache.Set(sh, status);
            await _subscriptions.NotifyScripthashAsync(sh, status);
        }

        if (newHeader is not null)
            await _subscriptions.NotifyHeadersAsync(newHeight, newHeader);

        _logger.LogInformation("Reorg complete. Now at height {Height}", newHeight);

        return newHeight;
    }

    /// <summary>
    ///     Recovers from a dirty state left by an interrupted sync/reorg mutation.
    /// </summary>
    /// <param name="dirtyState">Current dirty-state metadata.</param>
    /// <param name="syncedHeight">Current persisted synced height.</param>
    /// <returns>Recovered synced height.</returns>
    public async Task<int> RecoverDirtyStateAsync(DirtyState dirtyState, int syncedHeight)
    {
        if (!dirtyState.IsDirty)
            return syncedHeight;

        var baseHeight = dirtyState.BaseHeight
                         ?? throw new InvalidOperationException(
                             "Dirty state missing base height. Fatal inconsistent state.");

        if (dirtyState.OperationKind is DirtyOperationKind.None)
            throw new InvalidOperationException("Dirty state missing operation kind. Fatal inconsistent state.");

        switch (dirtyState.OperationKind)
        {
            case DirtyOperationKind.Sync:
            {
                if (syncedHeight < baseHeight)
                    throw new InvalidOperationException(
                        $"Dirty sync state inconsistent (synced={syncedHeight}, base={baseHeight}). Fatal.");

                if (syncedHeight > baseHeight)
                {
                    // Data was written during the interrupted mutation — rewind with safety margin
                    const int ExtraUndoBlocks = 6;
                    var safeHeight = Math.Max(-1, baseHeight - ExtraUndoBlocks);

                    _logger.LogWarning(
                        "Recovering interrupted sync: rewinding from {Current} to safe height {Safe} (base={Base}, extra={Extra})",
                        syncedHeight, safeHeight, baseHeight, ExtraUndoBlocks);
                    await UndoToHeightAsync(syncedHeight, safeHeight, null, true);
                    syncedHeight = _store.GetSyncedHeight();
                }
                else
                {
                    // syncedHeight == baseHeight: mutation started but wrote no new data
                    _logger.LogWarning(
                        "Recovering interrupted sync at base height {Base} (no data written)", baseHeight);
                }

                _store.EndMutation();
                return syncedHeight;
            }

            case DirtyOperationKind.Reorg:
            {
                if (syncedHeight > baseHeight)
                    throw new InvalidOperationException(
                        $"Dirty reorg state inconsistent (synced={syncedHeight}, base={baseHeight}). Fatal.");

                _logger.LogWarning(
                    "Recovering interrupted reorg: keeping synced height {Synced} (base was {Base})",
                    syncedHeight,
                    baseHeight);

                _store.EndMutation();
                return syncedHeight;
            }

            default:
                throw new InvalidOperationException(
                    $"Unknown dirty operation kind '{dirtyState.OperationKind}'. Fatal.");
        }
    }

    private async Task UndoToHeightAsync(int fromHeight, int toHeight, HashSet<string>? affectedScripthashes,
        bool bestEffort = false)
    {
        for (var heightToUndo = fromHeight; heightToUndo > toHeight; heightToUndo--)
        {
            var undoData = _store.GetUndoBlock(heightToUndo);
            if (undoData is null)
            {
                if (bestEffort)
                {
                    _logger.LogWarning(
                        "Missing undo data for block {Height}; stopping best-effort recovery at height {StopHeight}",
                        heightToUndo, heightToUndo);
                    return;
                }

                throw new InvalidOperationException(
                    $"Missing undo data for block {heightToUndo}. Cannot recover safely.");
            }

            _logger.LogDebug("Undoing block {Height}", heightToUndo);

            using var batch = _store.CreateWriteBatch();
            UnindexBlockFromUndo(heightToUndo, undoData, batch, affectedScripthashes);
            _store.SetSyncedHeight(batch, heightToUndo - 1);
            _store.Write(batch);
        }

        await _utxoCache.FlushToDbAsync();
    }

    /// <summary>
    ///     Unindexes a block from its undo data by reversing transactions and UTXO changes.
    /// </summary>
    /// <param name="height">Block height to unindex.</param>
    /// <param name="undoData">Serialized undo payload for the block.</param>
    /// <param name="batch">RocksDB write batch for atomic writes.</param>
    /// <param name="affectedScripthashes">Set to accumulate affected scripthashes.</param>
    private void UnindexBlockFromUndo(int height, byte[] undoData, WriteBatch batch,
        HashSet<string>? affectedScripthashes)
    {
        var undo = UndoBlock.Deserialize(undoData);

        foreach (var utxo in undo.SpentUtxos)
        {
            _utxoCache.Put(utxo.TxNum, utxo.OutputIndex, utxo.Scripthash, utxo.Value, utxo.Height);
            _store.PutScripthashUtxo(batch, utxo.Scripthash, utxo.TxNum, utxo.OutputIndex, utxo.Height, utxo.Value);
            _store.PutUtxo(batch, utxo.TxNum, utxo.OutputIndex, utxo.Scripthash, utxo.Value, utxo.Height);
            affectedScripthashes?.Add(ScriptHashUtil.ToHex(utxo.Scripthash));
        }

        foreach (var utxo in undo.CreatedUtxos)
        {
            _utxoCache.Delete(utxo.TxNum, utxo.OutputIndex);
            _store.DeleteScripthashUtxo(batch, utxo.Scripthash, utxo.TxNum, utxo.OutputIndex);
            _store.DeleteUtxo(batch, utxo.TxNum, utxo.OutputIndex);
            affectedScripthashes?.Add(ScriptHashUtil.ToHex(utxo.Scripthash));
        }

        var grouped = undo.HistoryEntries
            .GroupBy(h => h.Scripthash, ByteArrayComparer.Instance)
            .Select(g => new
            {
                Scripthash = g.Key,
                MinTxNum = g.Min(h => h.TxNum)
            });

        foreach (var group in grouped)
        {
            _store.ChunkedHistory.DeleteTxNumsAfter(batch, group.Scripthash, group.MinTxNum);
            affectedScripthashes?.Add(ScriptHashUtil.ToHex(group.Scripthash));
        }

        var header = _store.GetBlockHeader(height);
        if (header is not null)
        {
            var blockHash = Convert.FromHexString(BlockValidator.GetBlockHashFromHeader(header));
            _store.DeleteBlockHash(batch, blockHash);
        }

        foreach (var txHash in _store.TxNumManager.GetBlockTxHashes(height))
            _store.DeleteTransaction(batch, txHash);

        _store.DeleteBlockHeader(batch, height);
        _store.DeleteUndoBlock(batch, height);
        _store.TxNumManager.RewindToBlock(batch, height);
    }


    /// <summary>
    ///     Cleans up old undo blocks that are no longer needed for reorg recovery.
    /// </summary>
    /// <param name="batch">RocksDB write batch for atomic writes.</param>
    /// <param name="currentHeight">Current block height.</param>
    public void CleanupOldUndoBlocks(WriteBatch batch, int currentHeight)
    {
        var deleteUpTo = currentHeight - _maxReorgDepth;
        if (deleteUpTo <= 0) return;
        _store.DeleteUndoBlockRange(batch, 0, deleteUpTo + 1);
    }
}