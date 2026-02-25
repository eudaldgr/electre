using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Electre.Database;

/// <summary>
///     Double-buffered UTXO cache with asynchronous flushing to the database.
///     Maintains an active layer for writes and a flushing layer for background persistence.
/// </summary>
public sealed class UtxoCache : IDisposable
{
    /// <summary>
    ///     Threshold for triggering a flush operation.
    /// </summary>
    private readonly int _flushThreshold;

    /// <summary>
    ///     Hard cap (2x flush threshold) where backpressure kicks in.
    /// </summary>
    private readonly int _hardCap; // 2x flush threshold - backpressure kicks in here

    /// <summary>
    ///     Logger instance for diagnostic output.
    /// </summary>
    private readonly ILogger _logger;

    /// <summary>
    ///     Maximum number of entries allowed in the cache.
    /// </summary>
    private readonly int _maxEntries;

    /// <summary>
    ///     Store instance for database operations.
    /// </summary>
    private readonly Store _store;

    /// <summary>
    ///     Lock for synchronizing layer swaps.
    /// </summary>
    private readonly object _swapLock = new();

    /// <summary>
    ///     Active cache layer for writes.
    /// </summary>
    private volatile CacheLayer _activeLayer = new();

    /// <summary>
    ///     Flushing cache layer (read-only, being written to DB).
    /// </summary>
    private volatile CacheLayer? _flushingLayer;

    /// <summary>
    ///     Background flush task.
    /// </summary>
    private Task _flushTask = Task.CompletedTask;

    /// <summary>
    ///     Flag indicating initial block download (IBD) mode for non-blocking flushes.
    /// </summary>
    private volatile bool _ibdMode; // IBD mode: non-blocking flushes until hard cap

    /// <summary>
    ///     Count of pending delete operations.
    /// </summary>
    private int _totalPendingDeletes;

    /// <summary>
    ///     Count of pending write operations.
    /// </summary>
    private int _totalPendingWrites;

    /// <summary>
    ///     Initializes a new UtxoCache instance.
    /// </summary>
    /// <param name="store">Store instance for database operations.</param>
    /// <param name="loggerFactory">Logger factory for creating diagnostic loggers.</param>
    /// <param name="maxEntries">Maximum number of entries allowed in the cache (default: 10,000,000).</param>
    /// <param name="flushThreshold">Threshold for triggering a flush operation (default: 1,000,000).</param>
    public UtxoCache(Store store, ILoggerFactory loggerFactory, int maxEntries = 10_000_000,
        int flushThreshold = 1_000_000)
    {
        if (maxEntries <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxEntries), "maxEntries must be > 0");
        if (flushThreshold <= 0)
            throw new ArgumentOutOfRangeException(nameof(flushThreshold), "flushThreshold must be > 0");

        _logger = loggerFactory.CreateLogger("UtxoCache");
        _store = store;
        _maxEntries = maxEntries;
        _flushThreshold = Math.Min(flushThreshold, maxEntries);
        _hardCap = maxEntries;
    }

    /// <summary>
    ///     Gets the total number of entries in both active and flushing cache layers.
    /// </summary>
    public int CacheCount => _activeLayer.Count + (_flushingLayer?.Count ?? 0);

    /// <summary>
    ///     Disposes the cache, ensuring all pending flushes complete.
    /// </summary>
    public void Dispose()
    {
        Task flushTask;
        lock (_swapLock)
        {
            flushTask = _flushTask;
        }

        // Intentional sync-over-async: Dispose cannot be async, must block to ensure flush completes
        flushTask.Wait();
    }

    /// <summary>
    ///     Puts a UTXO into the cache.
    /// </summary>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="value">UTXO value in satoshis.</param>
    /// <param name="height">Block height containing the transaction.</param>
    public void Put(ulong txNum, int outputIndex, byte[] scripthash, long value, int height)
    {
        var key = new UtxoKey(txNum, outputIndex);
        var val = new UtxoValue(scripthash, value, height);

        _activeLayer.Put(key, val);
        Interlocked.Increment(ref _totalPendingWrites);

        CheckFlush();
        EnforceHardCapIfNeeded();
    }

    /// <summary>
    ///     Deletes a UTXO from the cache.
    /// </summary>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    public void Delete(ulong txNum, int outputIndex)
    {
        var key = new UtxoKey(txNum, outputIndex);

        _activeLayer.Delete(key);
        Interlocked.Increment(ref _totalPendingDeletes);

        CheckFlush();
        EnforceHardCapIfNeeded();
    }

    /// <summary>
    ///     Gets a UTXO from the cache or database.
    /// </summary>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    /// <returns>UTXO value or null if not found or deleted.</returns>
    public UtxoValue? Get(ulong txNum, int outputIndex)
    {
        var key = new UtxoKey(txNum, outputIndex);

        if (_activeLayer.TryGet(key, out var val, out var isDeleted))
            return isDeleted ? null : val;

        var flushing = _flushingLayer;
        if (flushing is not null)
            if (flushing.TryGet(key, out val, out isDeleted))
                return isDeleted ? null : val;

        var fromDb = _store.GetUtxo(txNum, outputIndex);
        if (fromDb is null)
            return null;

        return new UtxoValue(fromDb.Value.scripthash, fromDb.Value.value, fromDb.Value.height);
    }

    /// <summary>
    ///     Checks if a flush should be triggered based on cache size.
    /// </summary>
    private void CheckFlush(bool force = false)
    {
        if (!force && _activeLayer.Count < _flushThreshold)
            return;

        if (force && _activeLayer.IsEmpty)
            return;

        lock (_swapLock)
        {
            if (!force && _activeLayer.Count < _flushThreshold)
                return;

            if (force && _activeLayer.IsEmpty)
                return;

            _flushingLayer = _activeLayer;
            _activeLayer = new CacheLayer();

            _flushTask = Task.Run(DoFlushAsync);
        }
    }

    /// <summary>
    ///     Applies backpressure when pending entries exceed hard cap.
    /// </summary>
    private void EnforceHardCapIfNeeded()
    {
        if (CacheCount < _hardCap)
            return;

        CheckFlush(true);
    }

    /// <summary>
    ///     Performs the asynchronous flush operation.
    /// </summary>
    private void DoFlushAsync()
    {
        try
        {
            var layerToFlush = _flushingLayer;
            if (layerToFlush is null || layerToFlush.IsEmpty)
                return;

            var evicted = layerToFlush.Count;
            if (evicted > 0)
                _logger.LogDebug("Cache eviction: {Count} entries released from memory", evicted);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during cache eviction");
        }
    }

    /// <summary>
    ///     Ends the flush operation and clears the flushing layer.
    /// </summary>
    private void EndFlush()
    {
        lock (_swapLock)
        {
            _flushingLayer = null;
            Interlocked.Exchange(ref _totalPendingWrites, 0); // Approx reset
            Interlocked.Exchange(ref _totalPendingDeletes, 0);
        }
    }

    /// <summary>
    ///     Flushes the cache to the database asynchronously.
    /// </summary>
    /// <returns>A task representing the asynchronous flush operation.</returns>
    public async Task FlushToDbAsync()
    {
        var needsBackpressure = CacheCount >= _hardCap;

        if (_ibdMode && !needsBackpressure)
        {
            CheckFlush();
            return;
        }

        Task flushTask;
        lock (_swapLock)
        {
            flushTask = _flushTask;
        }

        await flushTask;
        EndFlush();

        lock (_swapLock)
        {
            if (_activeLayer.IsEmpty)
                return;

            _flushingLayer = _activeLayer;
            _activeLayer = new CacheLayer();
            _flushTask = Task.Run(DoFlushAsync);
            flushTask = _flushTask;
        }

        await flushTask;
        EndFlush();
    }

    /// <summary>
    ///     Clears all in-memory cache state. Call after crash recovery to discard
    ///     potentially stale data from undone blocks.
    /// </summary>
    public async Task ClearAllAsync()
    {
        Task flushTask;
        lock (_swapLock)
        {
            flushTask = _flushTask;
        }

        await flushTask;

        lock (_swapLock)
        {
            _activeLayer = new CacheLayer();
            _flushingLayer = null;
            _flushTask = Task.CompletedTask;
            Interlocked.Exchange(ref _totalPendingWrites, 0);
            Interlocked.Exchange(ref _totalPendingDeletes, 0);
        }

        _logger.LogInformation("UtxoCache cleared after recovery");
    }

    /// <summary>
    ///     Sets the initial block download (IBD) mode.
    /// </summary>
    /// <param name="enabled">True to enable IBD mode, false to disable.</param>
    public void SetIbdMode(bool enabled)
    {
        _ibdMode = enabled;
        _logger.LogInformation("UtxoCache IBD mode: {Enabled}", enabled);
    }

    /// <summary>
    ///     Waits for all pending flush operations to complete asynchronously.
    /// </summary>
    /// <returns>A task representing the wait operation.</returns>
    public async Task WaitForFlushCompleteAsync()
    {
        Task flushTask;
        lock (_swapLock)
        {
            flushTask = _flushTask;
        }

        await flushTask;

        lock (_swapLock)
        {
            if (!_activeLayer.IsEmpty)
            {
                _flushingLayer = _activeLayer;
                _activeLayer = new CacheLayer();
                _flushTask = Task.Run(DoFlushAsync);
                flushTask = _flushTask;
            }
        }

        await flushTask;
    }

    /// <summary>
    ///     Internal cache layer for double-buffered storage.
    /// </summary>
    private sealed class CacheLayer
    {
        /// <summary>
        ///     Dictionary of cached UTXO values.
        /// </summary>
        public readonly ConcurrentDictionary<UtxoKey, UtxoValue> Cache = new();

        /// <summary>
        ///     Dictionary of deleted UTXO markers.
        /// </summary>
        public readonly ConcurrentDictionary<UtxoKey, bool> Deleted = new();

        /// <summary>
        ///     Checks if the cache layer is empty.
        /// </summary>
        public bool IsEmpty => Cache.IsEmpty && Deleted.IsEmpty;

        /// <summary>
        ///     Gets the total number of entries (cached + deleted).
        /// </summary>
        public int Count => Cache.Count + Deleted.Count;

        /// <summary>
        ///     Puts a UTXO value into the cache layer.
        /// </summary>
        /// <param name="key">UTXO key.</param>
        /// <param name="value">UTXO value.</param>
        public void Put(UtxoKey key, UtxoValue value)
        {
            Deleted.TryRemove(key, out _);
            Cache[key] = value;
        }

        /// <summary>
        ///     Marks a UTXO as deleted in the cache layer.
        /// </summary>
        /// <param name="key">UTXO key.</param>
        public void Delete(UtxoKey key)
        {
            Cache.TryRemove(key, out _);
            Deleted[key] = true;
        }

        /// <summary>
        ///     Tries to get a UTXO from the cache layer.
        /// </summary>
        /// <param name="key">UTXO key.</param>
        /// <param name="val">Output UTXO value if found.</param>
        /// <param name="isDeleted">Output flag indicating if the UTXO is marked as deleted.</param>
        /// <returns>True if found in this layer (either as value or as deleted marker), false otherwise.</returns>
        public bool TryGet(UtxoKey key, out UtxoValue val, out bool isDeleted)
        {
            val = default;
            isDeleted = false;

            if (Deleted.ContainsKey(key))
            {
                isDeleted = true;
                return true;
            }

            if (Cache.TryGetValue(key, out var v))
            {
                val = v;
                return true;
            }

            return false;
        }
    }
}

/// <summary>
///     Key for a UTXO in the cache, combining transaction number and output index.
/// </summary>
public readonly struct UtxoKey : IEquatable<UtxoKey>
{
    /// <summary>
    ///     Transaction number (48-bit).
    /// </summary>
    public readonly ulong TxNum;

    /// <summary>
    ///     Output index within the transaction.
    /// </summary>
    public readonly int OutputIndex;

    /// <summary>
    ///     Cached hash code for the key.
    /// </summary>
    private readonly int _hashCode;

    /// <summary>
    ///     Initializes a new UtxoKey.
    /// </summary>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    public UtxoKey(ulong txNum, int outputIndex)
    {
        TxNum = txNum;
        OutputIndex = outputIndex;
        _hashCode = HashCode.Combine(txNum, outputIndex);
    }

    /// <summary>
    ///     Compares two UtxoKey instances for equality.
    /// </summary>
    /// <param name="other">The other UtxoKey to compare.</param>
    /// <returns>True if equal, false otherwise.</returns>
    public bool Equals(UtxoKey other)
    {
        return TxNum == other.TxNum && OutputIndex == other.OutputIndex;
    }

    /// <summary>
    ///     Compares this UtxoKey with another object for equality.
    /// </summary>
    /// <param name="obj">The object to compare.</param>
    /// <returns>True if equal, false otherwise.</returns>
    public override bool Equals(object? obj)
    {
        return obj is UtxoKey other && Equals(other);
    }

    /// <summary>
    ///     Gets the hash code for this UtxoKey.
    /// </summary>
    /// <returns>Hash code.</returns>
    public override int GetHashCode()
    {
        return _hashCode;
    }
}

/// <summary>
///     Value for a UTXO in the cache, containing scripthash, value, and height.
/// </summary>
public readonly struct UtxoValue
{
    /// <summary>
    ///     Script hash (32 bytes).
    /// </summary>
    public readonly byte[] Scripthash;

    /// <summary>
    ///     UTXO value in satoshis.
    /// </summary>
    public readonly long Value;

    /// <summary>
    ///     Block height containing the transaction.
    /// </summary>
    public readonly int Height;

    /// <summary>
    ///     Initializes a new UtxoValue.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="value">UTXO value in satoshis.</param>
    /// <param name="height">Block height containing the transaction.</param>
    public UtxoValue(byte[] scripthash, long value, int height)
    {
        Scripthash = scripthash;
        Value = value;
        Height = height;
    }
}