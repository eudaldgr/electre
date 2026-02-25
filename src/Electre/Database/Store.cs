using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RocksDbSharp;

namespace Electre.Database;

/// <summary>
///     Thrown when a scripthash history exceeds the configured maximum item limit.
/// </summary>
public sealed class HistoryLimitExceededException(long count, int limit) : Exception(
    $"History too large ({count} items, limit {limit})");

/// <summary>
///     Mutation operation kind tracked in metadata while database writes are in progress.
/// </summary>
public enum DirtyOperationKind : byte
{
    None = 0,
    Sync = 1,
    Reorg = 2
}

/// <summary>
///     Durable dirty-state metadata used for crash recovery decisions.
/// </summary>
/// <param name="IsDirty">Whether the database is marked dirty.</param>
/// <param name="OperationKind">In-progress operation kind.</param>
/// <param name="BaseHeight">Synced height before the operation started.</param>
public readonly record struct DirtyState(bool IsDirty, DirtyOperationKind OperationKind, int? BaseHeight);

/// <summary>
///     Main RocksDB persistence layer for blockchain data.
///     Manages 13 column families for efficient storage of headers, transactions, UTXOs, and metadata.
/// </summary>
public sealed class Store : IAsyncDisposable
{
    /// <summary>
    ///     Minimum database memory allocation in bytes (512 MB).
    /// </summary>
    private const long MinDbMem = 512L * 1024 * 1024;

    /// <summary>
    ///     Maximum database memory allocation in bytes (2 GB).
    /// </summary>
    private const long MaxDbMem = 2L * 1024 * 1024 * 1024; // 2 GB (Default max if not configured)

    /// <summary>
    ///     Metadata key for synced block height.
    /// </summary>
    private static readonly byte[] MetadataKeySyncedHeight = Encoding.UTF8.GetBytes("synced_height");

    /// <summary>
    ///     Metadata key for dirty flag indicating incomplete block processing.
    /// </summary>
    private static readonly byte[] MetadataKeyDirty = Encoding.UTF8.GetBytes("dirty");

    /// <summary>
    ///     Metadata key for dirty operation kind.
    /// </summary>
    private static readonly byte[] MetadataKeyDirtyOperation = Encoding.UTF8.GetBytes("dirty_operation");

    /// <summary>
    ///     Metadata key for base synced height before dirty operation started.
    /// </summary>
    private static readonly byte[] MetadataKeyDirtyBaseHeight = Encoding.UTF8.GetBytes("dirty_base_height");

    /// <summary>
    ///     Metadata key for database format version.
    /// </summary>
    private static readonly byte[] MetadataKeyDbFormatVersion = Encoding.UTF8.GetBytes("db_format_version");

    /// <summary>
    ///     Metadata key for initial block download (IBD) in-progress marker.
    /// </summary>
    private static readonly byte[] MetadataKeyIbdInProgress = Encoding.UTF8.GetBytes("ibd_in_progress");

    /// <summary>
    ///     Block cache for RocksDB performance optimization.
    /// </summary>
    private readonly Cache _blockCache;

    /// <summary>
    ///     Column family handle for block info (height -> first_txnum+tx_count).
    /// </summary>
    private readonly ColumnFamilyHandle _cfBlkinfo;

    /// <summary>
    ///     Column family handle for block hashes (hash -> height).
    /// </summary>
    private readonly ColumnFamilyHandle _cfBlockHashes;

    /// <summary>
    ///     Column family handle for block headers (height -> raw header).
    /// </summary>
    private readonly ColumnFamilyHandle _cfHeaders;

    /// <summary>
    ///     Column family handle for metadata (key -> value).
    /// </summary>
    private readonly ColumnFamilyHandle _cfMetadata;

    /// <summary>
    ///     Column family handle for scripthash history (legacy storage).
    /// </summary>
    private readonly ColumnFamilyHandle _cfScripthashHistory;

    /// <summary>
    ///     Column family handle for chunked history head pointers (scripthash -> chunk_head).
    /// </summary>
    private readonly ColumnFamilyHandle _cfScripthashHistoryHead;

    /// <summary>
    ///     Column family handle for scripthash UTXOs (scripthash+txnum+idx -> height+value).
    /// </summary>
    private readonly ColumnFamilyHandle _cfScripthashUtxo;

    /// <summary>
    ///     Column family handle for transactions (txhash -> height+position).
    /// </summary>
    private readonly ColumnFamilyHandle _cfTransactions;

    /// <summary>
    ///     Column family handle for TxHash to TxNum mapping (txhash_suffix -> txnum_list).
    /// </summary>
    private readonly ColumnFamilyHandle _cfTxHash2TxNum;

    /// <summary>
    ///     Column family handle for TxNum to TxHash mapping (txnum -> txhash).
    /// </summary>
    private readonly ColumnFamilyHandle _cfTxNum2TxHash;

    /// <summary>
    ///     Column family handle for block undo data (height -> undo_payload).
    /// </summary>
    private readonly ColumnFamilyHandle _cfUndo;

    /// <summary>
    ///     Column family handle for global UTXO set (txnum+idx -> scripthash+value+height).
    /// </summary>
    private readonly ColumnFamilyHandle _cfUtxoSet;

    /// <summary>
    ///     Manager for chunked scripthash transaction history storage.
    /// </summary>
    private readonly ChunkedHistory _chunkedHistory;

    /// <summary>
    ///     RocksDB database instance.
    /// </summary>
    private readonly RocksDb _db;

    /// <summary>
    ///     Write options for IBD (fast, non-crash-safe) database writes (WAL disabled).
    /// </summary>
    private readonly WriteOptions _ibdWriteOptions;

    /// <summary>
    ///     Logger instance for diagnostic output.
    /// </summary>
    private readonly ILogger _logger;

    /// <summary>
    ///     Write options for normal (crash-safe) database writes.
    /// </summary>
    private readonly WriteOptions _normalWriteOptions;

    /// <summary>
    ///     Manager for TxNum (48-bit transaction number) assignment and lookup.
    /// </summary>
    private readonly TxNumManager _txNumManager;

    /// <summary>
    ///     Flag indicating if IBD write options should be used.
    /// </summary>
    private bool _useIbdOptions;

    /// <summary>
    ///     Initializes a new Store instance with RocksDB database at the specified path.
    /// </summary>
    /// <param name="path">Directory path for the RocksDB database.</param>
    /// <param name="config">Configuration options for database memory and performance tuning.</param>
    /// <param name="loggerFactory">Logger factory for creating diagnostic loggers.</param>
    public Store(string path, IOptions<Config> config, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger("Store");
        var cfg = config.Value;
        Directory.CreateDirectory(path);

        var dbMem = CalculateDbMem(cfg.DbMem);
        var blockCacheSize = (ulong)(dbMem * 0.5);
        var writeBufferBudget = (ulong)(dbMem * 0.5);
        var writeBufferPerCf = writeBufferBudget / 13 / (ulong)cfg.DbMaxWriteBufferNumber;

        _logger.LogInformation("RocksDB Memory: {DbMem} MB (BlockCache: {Cache} MB, WriteBuffer: {Write} MB)",
            dbMem / 1024 / 1024, blockCacheSize / 1024 / 1024, writeBufferBudget / 1024 / 1024);
        _logger.LogInformation("Write Buffer per CF: {Mem} MB (Buffers: {Count})",
            writeBufferPerCf / 1024 / 1024, cfg.DbMaxWriteBufferNumber);

        _blockCache = Cache.CreateLru(blockCacheSize);
        var tableOptions = new BlockBasedTableOptions()
            .SetBlockCache(_blockCache)
            .SetFilterPolicy(BloomFilterPolicy.Create(10, false))
            .SetCacheIndexAndFilterBlocks(true)
            .SetPinL0FilterAndIndexBlocksInCache(true);

        var parallelism = cfg.DbParallelism > 0
            ? cfg.DbParallelism
            : Math.Min(4, Environment.ProcessorCount);

        var options = new DbOptions()
            .SetCreateIfMissing()
            .SetCreateMissingColumnFamilies()
            .IncreaseParallelism(parallelism)
            .SetCompression(Compression.Lz4)
            .SetMaxOpenFiles(cfg.DbMaxOpenFiles)
            .SetDbWriteBufferSize(writeBufferBudget)
            .SetBlockBasedTableFactory(tableOptions);

        // Ensure all column families flush atomically to prevent desync on crash
        rocksdb_options_set_atomic_flush(options.Handle, 1);

        var columnFamilyOptions = new ColumnFamilyOptions()
            .SetCompression(Compression.Lz4)
            .SetWriteBufferSize(writeBufferPerCf)
            .SetMaxWriteBufferNumber(cfg.DbMaxWriteBufferNumber)
            .SetBlockBasedTableFactory(tableOptions);

        var columnFamilies = new ColumnFamilies
        {
            { "default", columnFamilyOptions },
            { "block_headers", columnFamilyOptions },
            { "block_hashes", columnFamilyOptions },
            { "transactions", columnFamilyOptions },
            { "scripthash_history", columnFamilyOptions },
            { "scripthash_utxo", columnFamilyOptions },
            { "metadata", columnFamilyOptions },
            { "undo", columnFamilyOptions },
            { "utxo_set", columnFamilyOptions },
            { "blkinfo", columnFamilyOptions },
            { "txnum2txhash", columnFamilyOptions },
            { "txhash2txnum", columnFamilyOptions },
            { "scripthash_history_head", columnFamilyOptions }
        };

        _db = RocksDb.Open(options, path, columnFamilies);
        _normalWriteOptions = new WriteOptions();
        _ibdWriteOptions = new WriteOptions().DisableWal(1);

        _cfHeaders = _db.GetColumnFamily("block_headers");
        _cfBlockHashes = _db.GetColumnFamily("block_hashes");
        _cfTransactions = _db.GetColumnFamily("transactions");
        _cfScripthashHistory = _db.GetColumnFamily("scripthash_history");
        _cfScripthashUtxo = _db.GetColumnFamily("scripthash_utxo");
        _cfMetadata = _db.GetColumnFamily("metadata");
        _cfUndo = _db.GetColumnFamily("undo");
        _cfUtxoSet = _db.GetColumnFamily("utxo_set");
        _cfBlkinfo = _db.GetColumnFamily("blkinfo");
        _cfTxNum2TxHash = _db.GetColumnFamily("txnum2txhash");
        _cfTxHash2TxNum = _db.GetColumnFamily("txhash2txnum");
        _cfScripthashHistoryHead = _db.GetColumnFamily("scripthash_history_head");

        // Validate database format version
        ValidateDatabaseVersion();

        // Legacy compatibility: older versions could leave this marker behind.
        var ibdMarker = GetMetadata(MetadataKeyIbdInProgress);
        if (ibdMarker is [1])
        {
            _logger.LogWarning(
                "Found stale 'ibd_in_progress' marker from an older run. Clearing marker and continuing startup.");
            PutMetadata(MetadataKeyIbdInProgress, new byte[] { 0 });
        }

        // Initialize TxNumManager
        _txNumManager = new TxNumManager(_db, _cfBlkinfo, _cfTxNum2TxHash, _cfTxHash2TxNum, _cfMetadata,
            cfg.TxHashCacheSize);

        // Initialize ChunkedHistory
        _chunkedHistory = new ChunkedHistory(_db, _cfScripthashHistory, _cfScripthashHistoryHead);
    }

    /// <summary>
    ///     Gets the TxNum manager for transaction number operations.
    /// </summary>
    public TxNumManager TxNumManager => _txNumManager;

    /// <summary>
    ///     Gets the chunked history manager for scripthash history operations.
    /// </summary>
    public ChunkedHistory ChunkedHistory => _chunkedHistory;

    /// <summary>
    ///     Disposes the database connection asynchronously.
    /// </summary>
    /// <returns>A completed ValueTask.</returns>
    public ValueTask DisposeAsync()
    {
        _db.Dispose();
        return ValueTask.CompletedTask;
    }
    // --- P/Invoke for RocksDB options not exposed by RocksDbSharp ---

    [DllImport("rocksdb", CallingConvention = CallingConvention.Cdecl)]
    private static extern void rocksdb_options_set_atomic_flush(IntPtr options, byte value);

    [DllImport("rocksdb", CallingConvention = CallingConvention.Cdecl)]
    private static extern void rocksdb_flush_wal(IntPtr db, byte sync, out IntPtr errptr);

    [DllImport("rocksdb", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr rocksdb_flushoptions_create();

    [DllImport("rocksdb", CallingConvention = CallingConvention.Cdecl)]
    private static extern void rocksdb_flushoptions_set_wait(IntPtr options, byte value);

    [DllImport("rocksdb", CallingConvention = CallingConvention.Cdecl)]
    private static extern void rocksdb_flushoptions_destroy(IntPtr options);

    [DllImport("rocksdb", CallingConvention = CallingConvention.Cdecl)]
    private static extern void rocksdb_flush_cf(
        IntPtr db, IntPtr options, IntPtr columnFamily, out IntPtr errptr);

    /// <summary>
    ///     Validates the database format version and throws if incompatible.
    /// </summary>
    private void ValidateDatabaseVersion()
    {
        var versionBytes = GetMetadata(MetadataKeyDbFormatVersion);
        if (versionBytes is null)
        {
            // No version key - either old DB (version 1) or new empty DB
            var syncedHeight = GetSyncedHeight();
            if (syncedHeight >= 0)
                // DB has data but no version key - this is version 1
                throw new InvalidOperationException("DB is version 1, resync required for version 2");
            // New empty DB - write version 2
            var versionValue = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(versionValue, 2u);
            PutMetadata(MetadataKeyDbFormatVersion, versionValue);
        }
        else
        {
            // Version key exists - validate it
            var storedVersion = BinaryPrimitives.ReadUInt32BigEndian(versionBytes);
            if (storedVersion < 2)
                throw new InvalidOperationException($"DB version {storedVersion} outdated, resync required");

            if (storedVersion > 2)
                throw new InvalidOperationException($"DB version {storedVersion} too new, upgrade Electre");
        }
    }

    /// <summary>
    ///     Puts a block header at the specified height.
    /// </summary>
    /// <param name="height">Block height (4 bytes big-endian).</param>
    /// <param name="header">Raw block header (80 bytes).</param>
    public void PutBlockHeader(int height, byte[] header)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        _db.Put(key, header, _cfHeaders);
    }

    /// <summary>
    ///     Gets a block header at the specified height.
    /// </summary>
    /// <param name="height">Block height.</param>
    /// <returns>Raw block header (80 bytes) or null if not found.</returns>
    public byte[]? GetBlockHeader(int height)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        return _db.Get(key, _cfHeaders);
    }

    /// <summary>
    ///     Gets multiple block headers starting from the specified height.
    /// </summary>
    /// <param name="startHeight">Starting block height.</param>
    /// <param name="count">Number of headers to retrieve.</param>
    /// <returns>List of raw block headers (80 bytes each), stops at first missing header.</returns>
    public List<byte[]> GetBlockHeaders(int startHeight, int count)
    {
        if (count <= 0)
            return [];

        var keys = new byte[count][];
        var cfs = new ColumnFamilyHandle[count];
        for (var i = 0; i < count; i++)
        {
            var key = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(key, startHeight + i);
            keys[i] = key;
            cfs[i] = _cfHeaders;
        }

        var results = _db.MultiGet(keys, cfs);
        var headers = new List<byte[]>(count);
        foreach (var kv in results)
        {
            if (kv.Value is null) break;
            headers.Add(kv.Value);
        }

        return headers;
    }

    /// <summary>
    ///     Puts a block hash to height mapping.
    /// </summary>
    /// <param name="hash">Block hash (32 bytes).</param>
    /// <param name="height">Block height.</param>
    public void PutBlockHash(byte[] hash, int height)
    {
        var value = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(value, height);
        _db.Put(hash, value, _cfBlockHashes);
    }

    /// <summary>
    ///     Gets the block height for a given block hash.
    /// </summary>
    /// <param name="hash">Block hash (32 bytes).</param>
    /// <returns>Block height or null if not found.</returns>
    public int? GetBlockHeight(byte[] hash)
    {
        var value = _db.Get(hash, _cfBlockHashes);
        if (value is null)
            return null;

        return BinaryPrimitives.ReadInt32BigEndian(value);
    }

    /// <summary>
    ///     Puts a transaction with its block height and position.
    /// </summary>
    /// <param name="txHash">Transaction hash (32 bytes).</param>
    /// <param name="height">Block height containing the transaction.</param>
    /// <param name="position">Position of transaction within the block.</param>
    public void PutTransaction(byte[] txHash, int height, int position)
    {
        Span<byte> buf = stackalloc byte[20];
        var n1 = VarInt.Encode(height, buf);
        var n2 = VarInt.Encode(position, buf[n1..]);
        var value = buf[..(n1 + n2)].ToArray();
        _db.Put(txHash, value, _cfTransactions);
    }

    /// <summary>
    ///     Gets the block height and position for a transaction hash.
    /// </summary>
    /// <param name="txHash">Transaction hash (32 bytes).</param>
    /// <returns>Tuple of (height, position) or null if not found.</returns>
    public (int height, int position)? GetTransaction(byte[] txHash)
    {
        var value = _db.Get(txHash, _cfTransactions);
        if (value is null) return null;
        var (height, n1) = VarInt.DecodeInt32(value);
        var (position, _) = VarInt.DecodeInt32(value.AsSpan(n1));
        return (height, position);
    }

    /// <summary>
    ///     Gets all transactions for a scripthash in append order.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="maxItems">Maximum allowed history items. 0 means no limit.</param>
    /// <returns>List of (height, txHash) tuples in chronological order.</returns>
    /// <exception cref="HistoryLimitExceededException">Thrown when history exceeds maxItems.</exception>
    public List<(int height, byte[] txHash)> GetScripthashHistory(byte[] scripthash, int maxItems = 0)
    {
        if (maxItems > 0)
        {
            var count = _chunkedHistory.GetHistoryCount(scripthash);
            if (count > maxItems)
                throw new HistoryLimitExceededException(count, maxItems);
        }

        var txNums = _chunkedHistory.GetHistory(scripthash);
        if (txNums.Count == 0)
            return [];

        var txHashesByNum = _txNumManager.GetTxHashes(txNums);
        var heightsByNum = _txNumManager.GetHeightsForTxNums(txNums);
        var result = new List<(int height, byte[] txHash)>(txNums.Count);

        foreach (var txNum in txNums)
            if (txHashesByNum.TryGetValue(txNum, out var txHash) &&
                heightsByNum.TryGetValue(txNum, out var height))
                result.Add((height, txHash));

        return result;
    }

    /// <summary>
    ///     Gets all transactions for a scripthash in append order using batched txnum resolution.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <returns>List of (height, txHash) tuples in chronological order.</returns>
    public List<(int height, byte[] txHash)> GetScripthashHistoryResolved(byte[] scripthash)
    {
        return GetScripthashHistory(scripthash);
    }

    /// <summary>
    ///     Puts a UTXO for a scripthash using TxNum-based storage.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    /// <param name="height">Block height containing the transaction.</param>
    /// <param name="value">UTXO value in satoshis.</param>
    public void PutScripthashUtxo(WriteBatch batch, byte[] scripthash, ulong txNum, int outputIndex, int height,
        long value)
    {
        var key = new byte[42]; // scripthash (32B) + TxNum (6B) + idx (4B)
        scripthash.CopyTo(key, 0);

        // Write TxNum as 6 bytes LE
        TxNumCodec.Encode(key.AsSpan(32), txNum);

        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(38, 4), outputIndex);

        Span<byte> buf = stackalloc byte[20];
        var n1 = VarInt.Encode(height, buf);
        var n2 = VarInt.Encode(value, buf[n1..]);
        var val = buf[..(n1 + n2)].ToArray();

        batch.Put(key, val, _cfScripthashUtxo);
    }

    /// <summary>
    ///     Deletes a UTXO for a scripthash.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    public void DeleteScripthashUtxo(WriteBatch batch, byte[] scripthash, ulong txNum, int outputIndex)
    {
        var key = new byte[42];
        scripthash.CopyTo(key, 0);

        // Write TxNum as 6 bytes LE
        TxNumCodec.Encode(key.AsSpan(32), txNum);

        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(38, 4), outputIndex);
        batch.Delete(key, _cfScripthashUtxo);
    }

    /// <summary>
    ///     Gets a specific UTXO entry for a scripthash.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    /// <returns>Tuple of (height, value) or null if not found.</returns>
    public (int height, long value)? GetScripthashUtxoEntry(byte[] scripthash, ulong txNum, int outputIndex)
    {
        var key = new byte[42];
        scripthash.CopyTo(key, 0);

        // Write TxNum as 6 bytes LE
        TxNumCodec.Encode(key.AsSpan(32), txNum);

        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(38, 4), outputIndex);
        var val = _db.Get(key, _cfScripthashUtxo);
        if (val == null || val.Length == 0) return null;
        var (height, n1) = VarInt.DecodeInt32(val);
        var (value, _) = VarInt.DecodeInt64(val.AsSpan(n1));
        return (height, value);
    }

    /// <summary>
    ///     Gets all UTXOs for a scripthash.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <returns>List of (txNum, outputIndex, height, value) tuples.</returns>
    public List<(ulong txNum, int outputIndex, int height, long value)> GetScripthashUtxos(byte[] scripthash)
    {
        var result = new List<(ulong, int, int, long)>();

        using var iterator = _db.NewIterator(_cfScripthashUtxo);
        iterator.Seek(scripthash);

        while (iterator.Valid())
        {
            var key = iterator.Key();
            if (key.Length < 42 || !key.AsSpan(0, 32).SequenceEqual(scripthash))
                break;

            // Read TxNum from 6 bytes LE at offset 32
            var txNum = TxNumCodec.Decode(key.AsSpan(32, TxNumCodec.Size));

            var outputIndex = BinaryPrimitives.ReadInt32BigEndian(key.AsSpan(38, 4));
            var val = iterator.Value();
            var (height, n1) = VarInt.DecodeInt32(val);
            var (value, _) = VarInt.DecodeInt64(val.AsSpan(n1));

            result.Add((txNum, outputIndex, height, value));
            iterator.Next();
        }

        return result;
    }

    /// <summary>
    ///     Gets all UTXOs for a scripthash with TxHash resolved in batch.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <returns>List of (txHash, outputIndex, height, value) tuples.</returns>
    public List<(byte[] txHash, int outputIndex, int height, long value)> GetScripthashUtxosResolved(byte[] scripthash)
    {
        var utxos = GetScripthashUtxos(scripthash);
        if (utxos.Count == 0)
            return [];

        var txNums = utxos.Select(static u => u.txNum).ToArray();
        var txHashesByNum = _txNumManager.GetTxHashes(txNums);
        var result = new List<(byte[] txHash, int outputIndex, int height, long value)>(utxos.Count);

        foreach (var (txNum, outputIndex, height, value) in utxos)
            if (txHashesByNum.TryGetValue(txNum, out var txHash))
                result.Add((txHash, outputIndex, height, value));

        return result;
    }

    /// <summary>
    ///     Puts a metadata key-value pair.
    /// </summary>
    /// <param name="key">Metadata key.</param>
    /// <param name="value">Metadata value.</param>
    public void PutMetadata(string key, byte[] value)
    {
        _db.Put(Encoding.UTF8.GetBytes(key), value, _cfMetadata);
    }

    /// <summary>
    ///     Puts a metadata key-value pair using byte array key.
    /// </summary>
    /// <param name="key">Metadata key (byte array).</param>
    /// <param name="value">Metadata value.</param>
    private void PutMetadata(byte[] key, byte[] value)
    {
        _db.Put(key, value, _cfMetadata);
    }

    /// <summary>
    ///     Gets a metadata value by key.
    /// </summary>
    /// <param name="key">Metadata key.</param>
    /// <returns>Metadata value or null if not found.</returns>
    public byte[]? GetMetadata(string key)
    {
        return _db.Get(Encoding.UTF8.GetBytes(key), _cfMetadata);
    }

    /// <summary>
    ///     Gets a metadata value by byte array key.
    /// </summary>
    /// <param name="key">Metadata key (byte array).</param>
    /// <returns>Metadata value or null if not found.</returns>
    private byte[]? GetMetadata(byte[] key)
    {
        return _db.Get(key, _cfMetadata);
    }

    /// <summary>
    ///     Gets the last synced block height.
    /// </summary>
    /// <returns>Block height or -1 if not set.</returns>
    public int GetSyncedHeight()
    {
        var value = GetMetadata(MetadataKeySyncedHeight);
        if (value is null) return -1;
        return BinaryPrimitives.ReadInt32BigEndian(value);
    }

    /// <summary>
    ///     Sets the last synced block height.
    /// </summary>
    /// <param name="height">Block height to set.</param>
    public void SetSyncedHeight(int height)
    {
        var value = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(value, height);
        PutMetadata(MetadataKeySyncedHeight, value);
    }

    /// <summary>
    ///     Checks if the database is marked as dirty (incomplete block processing).
    /// </summary>
    /// <returns>True if dirty flag is set, false otherwise.</returns>
    public bool IsDirty()
    {
        var value = GetMetadata(MetadataKeyDirty);
        return value is [1];
    }

    /// <summary>
    ///     Gets the durable dirty-state metadata for crash recovery.
    /// </summary>
    /// <returns>Dirty-state snapshot.</returns>
    public DirtyState GetDirtyState()
    {
        var isDirty = IsDirty();
        if (!isDirty)
            return new DirtyState(false, DirtyOperationKind.None, null);

        var operation = DirtyOperationKind.None;
        var operationBytes = GetMetadata(MetadataKeyDirtyOperation);
        if (operationBytes is [var op] && Enum.IsDefined(typeof(DirtyOperationKind), op))
            operation = (DirtyOperationKind)op;

        int? baseHeight = null;
        var baseHeightBytes = GetMetadata(MetadataKeyDirtyBaseHeight);
        if (baseHeightBytes is { Length: 4 })
            baseHeight = BinaryPrimitives.ReadInt32BigEndian(baseHeightBytes);

        return new DirtyState(true, operation, baseHeight);
    }

    /// <summary>
    ///     Sets the dirty flag indicating incomplete block processing.
    /// </summary>
    /// <param name="isDirty">True to mark as dirty, false to clear.</param>
    public void SetDirty(bool isDirty)
    {
        PutMetadata(MetadataKeyDirty, new[] { isDirty ? (byte)1 : (byte)0 });
    }

    /// <summary>
    ///     Marks the beginning of a crash-recoverable mutation.
    ///     This write is always WAL-protected.
    /// </summary>
    /// <param name="operationKind">Mutation operation kind.</param>
    /// <param name="baseHeight">Synced height before mutation starts.</param>
    public void BeginMutation(DirtyOperationKind operationKind, int baseHeight)
    {
        if (operationKind is DirtyOperationKind.None)
            throw new ArgumentOutOfRangeException(nameof(operationKind), "Operation kind must not be None.");
        if (baseHeight < -1)
            throw new ArgumentOutOfRangeException(nameof(baseHeight), "Base height must be >= -1.");

        if (IsDirty())
            throw new InvalidOperationException("Cannot begin mutation while database is already dirty.");

        using var batch = CreateWriteBatch();
        SetDirty(batch, true);
        batch.Put(MetadataKeyDirtyOperation, new[] { (byte)operationKind }, _cfMetadata);
        var baseHeightBytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(baseHeightBytes, baseHeight);
        batch.Put(MetadataKeyDirtyBaseHeight, baseHeightBytes, _cfMetadata);
        Write(batch, true);
    }

    /// <summary>
    ///     Marks the successful end of a mutation and clears dirty metadata.
    ///     This write is always WAL-protected.
    /// </summary>
    public void EndMutation()
    {
        using var batch = CreateWriteBatch();
        SetDirty(batch, false);
        batch.Delete(MetadataKeyDirtyOperation, _cfMetadata);
        batch.Delete(MetadataKeyDirtyBaseHeight, _cfMetadata);
        Write(batch, true);
    }

    /// <summary>
    ///     Runs an operation while marking the database as dirty.
    ///     Dirty is cleared only after the operation completes successfully.
    /// </summary>
    /// <param name="operation">Operation to execute while dirty.</param>
    public async Task RunWithDirtyFlagAsync(Func<Task> operation)
    {
        BeginMutation(DirtyOperationKind.Sync, GetSyncedHeight());
        await operation();
        EndMutation();
    }

    /// <summary>
    ///     Creates a new write batch for atomic operations.
    /// </summary>
    /// <returns>A new WriteBatch instance.</returns>
    public WriteBatch CreateWriteBatch()
    {
        return new WriteBatch();
    }

    /// <summary>
    ///     Writes a batch of operations to the database atomically.
    /// </summary>
    /// <param name="batch">Write batch containing operations to apply.</param>
    public void Write(WriteBatch batch)
    {
        _db.Write(batch, _useIbdOptions ? _ibdWriteOptions : _normalWriteOptions);
    }

    /// <summary>
    ///     Writes a batch with optional forced WAL regardless of IBD mode.
    /// </summary>
    /// <param name="batch">Write batch containing operations to apply.</param>
    /// <param name="forceWal">True to force WAL-protected write.</param>
    public void Write(WriteBatch batch, bool forceWal)
    {
        _db.Write(batch, forceWal ? _normalWriteOptions : _useIbdOptions ? _ibdWriteOptions : _normalWriteOptions);
    }

    /// <summary>
    ///     Forces an fsync of the Write-Ahead Log to disc.
    ///     Call after batch writes in normal (non-IBD) mode to guarantee persistence.
    /// </summary>
    public void SyncWal()
    {
        rocksdb_flush_wal(_db.Handle, 1, out var errptr);
        if (errptr != IntPtr.Zero)
            // RocksDbException takes the IntPtr and handles freeing it
            throw new RocksDbException(errptr);
    }

    /// <summary>
    ///     Flushes all in-memory memtables to SST files on disk.
    ///     Essential during IBD shutdown since WAL is disabled and data only lives in memtables.
    ///     Uses rocksdb_flush_cf on the metadata CF (which always has data: synced_height, dirty flags).
    ///     With atomic_flush enabled, flushing one CF with data triggers an atomic flush of ALL CFs.
    /// </summary>
    public void FlushMemTables()
    {
        var flushOpts = rocksdb_flushoptions_create();
        try
        {
            rocksdb_flushoptions_set_wait(flushOpts, 1);
            rocksdb_flush_cf(_db.Handle, flushOpts, _cfMetadata.Handle, out var errptr);
            if (errptr != IntPtr.Zero)
                throw new RocksDbException(errptr);

            _logger.LogInformation("Flushed all memtables to disk");
        }
        finally
        {
            rocksdb_flushoptions_destroy(flushOpts);
        }
    }

    /// <summary>
    ///     Sets initial block download (IBD) mode.
    ///     When enabled, writes bypass the WAL (Write-Ahead Log) for performance.
    /// </summary>
    /// <param name="enabled">True to enable IBD mode (no WAL), false to disable (crash-safe).</param>
    public void SetIbdMode(bool enabled)
    {
        if (_useIbdOptions == enabled)
            return;

        _useIbdOptions = enabled;
        _logger.LogInformation("IBD Mode: {Status} (WAL {WalStatus})",
            enabled ? "Enabled" : "Disabled",
            enabled ? "Disabled" : "Enabled");

        PutMetadata(MetadataKeyIbdInProgress, new[] { enabled ? (byte)1 : (byte)0 });
    }

    /// <summary>
    ///     Puts a block header in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="height">Block height.</param>
    /// <param name="header">Raw block header (80 bytes).</param>
    public void PutBlockHeader(WriteBatch batch, int height, byte[] header)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        batch.Put(key, header, _cfHeaders);
    }

    /// <summary>
    ///     Puts a block hash to height mapping in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="hash">Block hash (32 bytes).</param>
    /// <param name="height">Block height.</param>
    public void PutBlockHash(WriteBatch batch, byte[] hash, int height)
    {
        var value = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(value, height);
        batch.Put(hash, value, _cfBlockHashes);
    }

    /// <summary>
    ///     Puts a transaction in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="txHash">Transaction hash (32 bytes).</param>
    /// <param name="height">Block height containing the transaction.</param>
    /// <param name="position">Position of transaction within the block.</param>
    public void PutTransaction(WriteBatch batch, byte[] txHash, int height, int position)
    {
        Span<byte> buf = stackalloc byte[20];
        var n1 = VarInt.Encode(height, buf);
        var n2 = VarInt.Encode(position, buf[n1..]);
        var value = buf[..(n1 + n2)].ToArray();
        batch.Put(txHash, value, _cfTransactions);
    }

    /// <summary>
    ///     Puts a scripthash transaction history entry in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="txNum">Transaction number (48-bit).</param>
    public void PutScripthashHistory(WriteBatch batch, byte[] scripthash, ulong txNum)
    {
        _chunkedHistory.AppendTxNum(batch, scripthash, txNum);
    }

    /// <summary>
    ///     Puts multiple scripthash history entries in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="entries">Sequence of (scripthash, txNum) entries.</param>
    public void PutScripthashHistoryBatch(WriteBatch batch, IReadOnlyList<(byte[] scripthash, ulong txNum)> entries)
    {
        if (entries.Count == 0)
            return;

        _chunkedHistory.AppendTxNumsBatch(batch, entries);
    }

    /// <summary>
    ///     Sets the synced height in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="height">Block height to set.</param>
    public void SetSyncedHeight(WriteBatch batch, int height)
    {
        var value = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(value, height);
        batch.Put(MetadataKeySyncedHeight, value, _cfMetadata);
    }

    /// <summary>
    ///     Sets the dirty flag in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="isDirty">True to mark as dirty, false to clear.</param>
    public void SetDirty(WriteBatch batch, bool isDirty)
    {
        var value = new[] { isDirty ? (byte)1 : (byte)0 };
        batch.Put(MetadataKeyDirty, value, _cfMetadata);
    }

    /// <summary>
    ///     Puts an undo block in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="height">Block height.</param>
    /// <param name="undoPayload">Undo data for block reorg.</param>
    public void PutUndoBlock(WriteBatch batch, int height, byte[] undoPayload)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        batch.Put(key, undoPayload, _cfUndo);
    }

    /// <summary>
    ///     Gets an undo block by height.
    /// </summary>
    /// <param name="height">Block height.</param>
    /// <returns>Undo data or null if not found.</returns>
    public byte[]? GetUndoBlock(int height)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        return _db.Get(key, _cfUndo);
    }

    /// <summary>
    ///     Deletes an undo block in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="height">Block height.</param>
    public void DeleteUndoBlock(WriteBatch batch, int height)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        batch.Delete(key, _cfUndo);
    }

    /// <summary>
    ///     Deletes a range of undo blocks in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="fromHeight">Starting block height (inclusive).</param>
    /// <param name="toHeightExclusive">Ending block height (exclusive).</param>
    public void DeleteUndoBlockRange(WriteBatch batch, int fromHeight, int toHeightExclusive)
    {
        var startKey = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(startKey, fromHeight);
        var endKey = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(endKey, toHeightExclusive);
        batch.DeleteRange(startKey, (ulong)startKey.Length, endKey, (ulong)endKey.Length, _cfUndo);
    }

    /// <summary>
    ///     Deletes a scripthash history entry in a write batch (legacy method).
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="height">Block height.</param>
    /// <param name="position">Transaction position within block.</param>
    public void DeleteScripthashHistory(WriteBatch batch, byte[] scripthash, int height, int position)
    {
        var key = new byte[40];
        scripthash.CopyTo(key, 0);
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(32, 4), height);
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(36, 4), position);
        batch.Delete(key, _cfScripthashHistory);
    }

    /// <summary>
    ///     Deletes a transaction in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="txHash">Transaction hash (32 bytes).</param>
    public void DeleteTransaction(WriteBatch batch, byte[] txHash)
    {
        batch.Delete(txHash, _cfTransactions);
    }

    /// <summary>
    ///     Deletes a block header in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="height">Block height.</param>
    public void DeleteBlockHeader(WriteBatch batch, int height)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        batch.Delete(key, _cfHeaders);
    }

    /// <summary>
    ///     Deletes a block hash mapping in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="hash">Block hash (32 bytes).</param>
    public void DeleteBlockHash(WriteBatch batch, byte[] hash)
    {
        batch.Delete(hash, _cfBlockHashes);
    }

    /// <summary>
    ///     Puts a UTXO in the global UTXO set in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="value">UTXO value in satoshis.</param>
    /// <param name="height">Block height containing the transaction.</param>
    public void PutUtxo(WriteBatch batch, ulong txNum, int outputIndex, byte[] scripthash, long value, int height)
    {
        var key = new byte[10];

        TxNumCodec.Encode(key.AsSpan(0), txNum);

        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(6, 4), outputIndex);

        Span<byte> buf = stackalloc byte[52];
        scripthash.CopyTo(buf);
        var n1 = VarInt.Encode(value, buf[32..]);
        var n2 = VarInt.Encode(height, buf[(32 + n1)..]);
        var val = buf[..(32 + n1 + n2)].ToArray();

        batch.Put(key, val, _cfUtxoSet);
    }

    /// <summary>
    ///     Gets a UTXO from the global UTXO set.
    /// </summary>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    /// <returns>Tuple of (scripthash, value, height) or null if not found.</returns>
    public (byte[] scripthash, long value, int height)? GetUtxo(ulong txNum, int outputIndex)
    {
        var key = new byte[10];

        TxNumCodec.Encode(key.AsSpan(0), txNum);

        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(6, 4), outputIndex);

        var val = _db.Get(key, _cfUtxoSet);
        if (val is null || val.Length < 33)
            return null;

        var scripthash = val[..32];
        var (value, n1) = VarInt.DecodeInt64(val.AsSpan(32));
        var (height, _) = VarInt.DecodeInt32(val.AsSpan(32 + n1));
        return (scripthash, value, height);
    }

    /// <summary>
    ///     Deletes a UTXO from the global UTXO set in a write batch.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="txNum">Transaction number (48-bit).</param>
    /// <param name="outputIndex">Output index within the transaction.</param>
    public void DeleteUtxo(WriteBatch batch, ulong txNum, int outputIndex)
    {
        var key = new byte[10];

        TxNumCodec.Encode(key.AsSpan(0), txNum);

        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(6, 4), outputIndex);
        batch.Delete(key, _cfUtxoSet);
    }

    /// <summary>
    ///     Calculates the database memory allocation based on configuration or system RAM.
    /// </summary>
    /// <param name="configuredMib">
    ///     Configured memory in MiB. Null or <= 0 uses automatic calculation.</param>
    /// <returns>Memory allocation in bytes.</returns>
    private long CalculateDbMem(double? configuredMib)
    {
        if (configuredMib.HasValue && configuredMib.Value > 0)
            return (long)(configuredMib.Value * 1024 * 1024);

        var totalRam = GetTotalPhysicalMemory();
        var quarterRam = totalRam / 4;
        var dbMem = Math.Min(MaxDbMem, quarterRam);
        return Math.Max(MinDbMem, dbMem);
    }

    /// <summary>
    ///     Gets the total physical memory available on the system.
    /// </summary>
    /// <returns>Total memory in bytes.</returns>
    private long GetTotalPhysicalMemory()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            try
            {
                var psi = new ProcessStartInfo
                {
                    FileName = "sysctl",
                    Arguments = "-n hw.memsize",
                    RedirectStandardOutput = true,
                    UseShellExecute = false
                };
                using var proc = Process.Start(psi);
                if (proc is not null)
                {
                    var output = proc.StandardOutput.ReadToEnd().Trim();
                    proc.WaitForExit();
                    if (long.TryParse(output, out var mem))
                        return mem;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error during compaction cleanup");
            }

        try
        {
            var memInfo = GC.GetGCMemoryInfo();
            return memInfo.TotalAvailableMemoryBytes;
        }
        catch
        {
            return 8L * 1024 * 1024 * 1024;
        }
    }
}