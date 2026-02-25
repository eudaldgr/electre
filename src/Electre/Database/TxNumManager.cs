using System.Buffers.Binary;
using System.Text;
using Electre.Utils;
using RocksDbSharp;

namespace Electre.Database;

/// <summary>
///     Manages TxNum (48-bit transaction numbers) for efficient storage.
///     TxNum is a monotonically increasing identifier assigned to each transaction.
/// </summary>
/// <summary>
///     Manages TxNum (48-bit transaction numbers) for efficient storage and lookup.
///     TxNum is a monotonically increasing identifier assigned to each transaction.
/// </summary>
public sealed class TxNumManager
{
    /// <summary>
    ///     Maximum value for a 48-bit transaction number (281,474,976,710,655).
    /// </summary>
    private const ulong TxNumMax = (1UL << 48) - 1; // 281,474,976,710,655

    /// <summary>
    ///     Synchronization for blkinfo index build/invalidation.
    /// </summary>
    private readonly object _blkinfoIndexLock = new();

    /// <summary>
    ///     Column family handle for block info (height -> first_txnum+tx_count).
    /// </summary>
    private readonly ColumnFamilyHandle _cfBlkinfo;

    /// <summary>
    ///     Column family handle for metadata (key -> value).
    /// </summary>
    private readonly ColumnFamilyHandle _cfMetadata;

    /// <summary>
    ///     Column family handle for TxHash to TxNum mapping (txhash_suffix -> txnum_list).
    /// </summary>
    private readonly ColumnFamilyHandle _cfTxHash2TxNum;

    /// <summary>
    ///     Column family handle for TxNum to TxHash mapping (txnum -> txhash).
    /// </summary>
    private readonly ColumnFamilyHandle _cfTxNum2TxHash;

    /// <summary>
    ///     RocksDB database instance.
    /// </summary>
    private readonly RocksDb _db;

    /// <summary>
    ///     Batch-local cache of txhash suffix buckets (txhash_suffix -> encoded txnum list).
    ///     Avoids same-batch read-after-write gaps on suffix collisions.
    /// </summary>
    private readonly Dictionary<byte[], byte[]> _recentTxHashBuckets = new(ByteArrayComparer.Instance);

    /// <summary>
    ///     Cache of recently assigned TxNums for fast lookup during batch processing.
    /// </summary>
    private readonly Dictionary<byte[], ulong> _recentTxNums = new(ByteArrayComparer.Instance);

    /// <summary>
    ///     Initializes a new TxNumManager instance.
    /// </summary>
    /// <param name="db">RocksDB database instance.</param>
    /// <param name="cfBlkinfo">Column family handle for block info.</param>
    /// <param name="cfTxNum2TxHash">Column family handle for TxNum to TxHash mapping.</param>
    /// <param name="cfTxHash2TxNum">Column family handle for TxHash to TxNum mapping.</param>
    /// <param name="cfMetadata">Column family handle for metadata.</param>
    /// <summary>
    ///     Optional LRU cache for TxNum→TxHash lookups. TxHash is immutable once written,
    ///     so no invalidation is needed.
    /// </summary>
    private readonly LruCache<ulong, byte[]>? _txHashCache;

    /// <summary>
    ///     Flag indicating whether a batch is currently active.
    /// </summary>
    private bool _batchActive;

    /// <summary>
    ///     Cached blkinfo index sorted by first TxNum for fast height lookups.
    /// </summary>
    private List<(ulong firstTxNum, int height)>? _blkinfoIndex;

    /// <summary>
    ///     Next TxNum to assign (in-memory cache during batch processing).
    /// </summary>
    private ulong _nextTxNumInMemory;

    public TxNumManager(
        RocksDb db,
        ColumnFamilyHandle cfBlkinfo,
        ColumnFamilyHandle cfTxNum2TxHash,
        ColumnFamilyHandle cfTxHash2TxNum,
        ColumnFamilyHandle cfMetadata,
        int txHashCacheSize = 0)
    {
        _db = db;
        _cfBlkinfo = cfBlkinfo;
        _cfTxNum2TxHash = cfTxNum2TxHash;
        _cfTxHash2TxNum = cfTxHash2TxNum;
        _cfMetadata = cfMetadata;

        if (txHashCacheSize > 0)
            _txHashCache = new LruCache<ulong, byte[]>(txHashCacheSize);
    }

    /// <summary>
    ///     Starts a batch operation, loading the next TxNum from the database.
    /// </summary>
    public void StartBatch()
    {
        if (!_batchActive)
        {
            _nextTxNumInMemory = GetNextTxNumFromDb();
            _batchActive = true;
        }
    }

    /// <summary>
    ///     Commits a batch operation, clearing the recent TxNum cache.
    /// </summary>
    public void CommitBatch()
    {
        _recentTxNums.Clear();
        _recentTxHashBuckets.Clear();
        _batchActive = false;
    }

    /// <summary>
    ///     Assigns a TxNum to a transaction and writes to all relevant CFs atomically.
    /// </summary>
    public ulong AssignTxNum(WriteBatch batch, byte[] txHash, int height, int txPosition)
    {
        StartBatch();
        var txNum = GetNextTxNum();

        if (txNum > TxNumMax)
            throw new InvalidOperationException($"TxNum overflow: {txNum} > {TxNumMax}");

        // Write txnum2txhash: TxNum -> TxHash
        var txNumKey = TxNumToBytes(txNum);
        batch.Put(txNumKey, txHash, _cfTxNum2TxHash);

        // Write txhash2txnum: Last 6B of TxHash -> List of TxNums
        var txHashKey = GetTxHashKey(txHash);
        _recentTxHashBuckets.TryGetValue(txHashKey, out var existingTxNums);
        existingTxNums ??= _db.Get(txHashKey, _cfTxHash2TxNum);

        byte[] newValue;
        if (existingTxNums is null)
        {
            // First TxNum for this hash suffix
            Span<byte> buf = stackalloc byte[20];
            var n = VarInt.Encode(txNum, buf);
            newValue = buf[..n].ToArray();
        }
        else
        {
            // Append to existing list
            Span<byte> buf = stackalloc byte[existingTxNums.Length + 20];
            existingTxNums.CopyTo(buf);
            var n = VarInt.Encode(txNum, buf[existingTxNums.Length..]);
            newValue = buf[..(existingTxNums.Length + n)].ToArray();
        }

        batch.Put(txHashKey, newValue, _cfTxHash2TxNum);
        _recentTxHashBuckets[txHashKey] = newValue;

        // Increment next_txnum
        IncrementNextTxNum(batch);

        _recentTxNums[txHash] = txNum;

        return txNum;
    }

    /// <summary>
    ///     Gets TxHash from TxNum.
    /// </summary>
    public byte[]? GetTxHash(ulong txNum)
    {
        if (_txHashCache is not null &&
            _txHashCache.TryGet(txNum, out var cached) && cached is not null)
            return cached;

        var key = TxNumToBytes(txNum);
        var value = _db.Get(key, _cfTxNum2TxHash);

        if (value is not null)
            _txHashCache?.Set(txNum, value);

        return value;
    }

    /// <summary>
    ///     Gets multiple TxHashes from TxNums in a single batch call.
    /// </summary>
    public Dictionary<ulong, byte[]> GetTxHashes(IReadOnlyList<ulong> txNums)
    {
        if (txNums.Count == 0)
            return [];

        var result = new Dictionary<ulong, byte[]>(txNums.Count);
        List<(int idx, ulong txNum)>? misses = null;

        // Check cache first
        if (_txHashCache is not null)
        {
            for (var i = 0; i < txNums.Count; i++)
                if (_txHashCache.TryGet(txNums[i], out var cached) && cached is not null)
                    result[txNums[i]] = cached;
                else
                    (misses ??= []).Add((i, txNums[i]));
            if (misses is null) return result; // All cache hits
        }

        // RocksDB MultiGet for misses only
        var queryNums = misses is not null
            ? misses.Select(m => m.txNum).ToArray()
            : (IReadOnlyList<ulong>)txNums;

        var keys = new byte[queryNums.Count][];
        var cfs = new ColumnFamilyHandle[queryNums.Count];
        for (var i = 0; i < queryNums.Count; i++)
        {
            keys[i] = TxNumToBytes(queryNums[i]);
            cfs[i] = _cfTxNum2TxHash;
        }

        var values = _db.MultiGet(keys, cfs);
        var idx = 0;
        foreach (var kv in values)
        {
            if (kv.Value is not null)
            {
                result[queryNums[idx]] = kv.Value;
                _txHashCache?.Set(queryNums[idx], kv.Value);
            }

            idx++;
        }

        return result;
    }

    /// <summary>
    ///     Gets TxNum from TxHash. Handles collisions by checking full hash.
    /// </summary>
    public ulong? GetTxNum(byte[] txHash)
    {
        if (_recentTxNums.TryGetValue(txHash, out var recentTxNum))
            return recentTxNum;

        var key = GetTxHashKey(txHash);
        var value = _db.Get(key, _cfTxHash2TxNum);

        if (value is null)
            return null;

        // Decode list of TxNums
        var offset = 0;
        while (offset < value.Length)
        {
            var (txNum, bytesRead) = VarInt.Decode(value.AsSpan(offset));
            offset += bytesRead;

            // Verify full hash match
            var storedHash = GetTxHash(txNum);
            if (storedHash is not null && storedHash.AsSpan().SequenceEqual(txHash))
                return txNum;
        }

        return null;
    }

    /// <summary>
    ///     Gets block height for a TxNum via binary search of blkinfo.
    /// </summary>
    public int? GetHeightForTxNum(ulong txNum)
    {
        var index = GetOrBuildBlkinfoIndex();
        if (index.Count == 0)
            return null;

        var low = 0;
        var high = index.Count - 1;
        var match = -1;

        while (low <= high)
        {
            var mid = low + (high - low) / 2;
            var candidate = index[mid];
            if (candidate.firstTxNum <= txNum)
            {
                match = mid;
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        if (match < 0)
            return null;

        var upperExclusive = match + 1 < index.Count
            ? index[match + 1].firstTxNum
            : GetNextTxNumFromDb();

        if (txNum >= upperExclusive)
            return null;

        return index[match].height;
    }

    /// <summary>
    ///     Gets block heights for multiple TxNums with a single index scan.
    /// </summary>
    public Dictionary<ulong, int> GetHeightsForTxNums(IReadOnlyList<ulong> txNums)
    {
        if (txNums.Count == 0)
            return [];

        var index = GetOrBuildBlkinfoIndex();
        if (index.Count == 0)
            return [];

        var ordered = txNums.Distinct().OrderBy(static x => x).ToArray();
        if (ordered.Length == 0)
            return [];

        var result = new Dictionary<ulong, int>(ordered.Length);
        var nextTxNum = GetNextTxNumFromDb();
        var idx = FindFirstBlockIndexForTxNum(index, ordered[0]);
        if (idx < 0)
            return result;

        foreach (var txNum in ordered)
        {
            while (idx + 1 < index.Count && index[idx + 1].firstTxNum <= txNum)
                idx++;

            var upperExclusive = idx + 1 < index.Count
                ? index[idx + 1].firstTxNum
                : nextTxNum;

            if (index[idx].firstTxNum <= txNum && txNum < upperExclusive)
                result[txNum] = index[idx].height;
        }

        return result;
    }

    /// <summary>
    ///     Gets all transaction hashes for a specific block height using blkinfo metadata.
    /// </summary>
    public List<byte[]> GetBlockTxHashes(int height)
    {
        if (height < 0)
            return [];

        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        var value = _db.Get(key, _cfBlkinfo);
        if (value is null || value.Length < 6)
            return [];

        var firstTxNum = TxNumFromBytes(value.AsSpan(0, 6));
        var (txCount, _) = VarInt.DecodeInt32(value.AsSpan(6));
        if (txCount <= 0)
            return [];

        var result = new List<byte[]>(txCount);
        for (var i = 0; i < txCount; i++)
        {
            var txHash = GetTxHash(firstTxNum + (ulong)i);
            if (txHash is not null)
                result.Add(txHash);
        }

        return result;
    }

    /// <summary>
    ///     Prepares block metadata (called at start of block processing).
    /// </summary>
    public void StartBlock(WriteBatch batch, int height)
    {
        // Block metadata will be written in FinishBlock
        // This method is a placeholder for future use
    }

    /// <summary>
    ///     Finalizes block metadata with first TxNum and count.
    /// </summary>
    public void FinishBlock(WriteBatch batch, int height, int txCount, ulong firstTxNum)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);

        Span<byte> value = stackalloc byte[6 + 20]; // 6B firstTxNum + VarInt txCount
        TxNumToBytes(firstTxNum).CopyTo(value);
        var n = VarInt.Encode(txCount, value[6..]);

        batch.Put(key, value[..(6 + n)].ToArray(), _cfBlkinfo);
        InvalidateBlkinfoIndex();
    }

    /// <summary>
    ///     Rewinds to a block height, deleting all TxNums >= first_txnum_of_height.
    /// </summary>
    public void RewindToBlock(WriteBatch batch, int height)
    {
        if (height < 0)
            return;

        // Get first TxNum of the block to rewind to
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, height);
        var value = _db.Get(key, _cfBlkinfo);

        if (value is null || value.Length < 6)
            return;

        var firstTxNum = TxNumFromBytes(value.AsSpan(0, 6));

        var txHashBucketCache = new Dictionary<byte[], byte[]?>(ByteArrayComparer.Instance);

        // Delete all txnum2txhash entries >= firstTxNum
        using (var iterator = _db.NewIterator(_cfTxNum2TxHash))
        {
            var startKey = TxNumToBytes(firstTxNum);
            iterator.Seek(startKey);

            while (iterator.Valid())
            {
                var txNumKey = iterator.Key();
                var txHash = iterator.Value();
                var txNum = TxNumFromBytes(txNumKey);

                // Evict from LRU cache
                _txHashCache?.Remove(txNum);

                // Delete from txhash2txnum
                var txHashKey = GetTxHashKey(txHash);
                if (!txHashBucketCache.TryGetValue(txHashKey, out var bucketValue))
                    bucketValue = _db.Get(txHashKey, _cfTxHash2TxNum);

                if (bucketValue is not null)
                {
                    var updatedBucket = RemoveTxNumFromBucket(bucketValue, txNum);
                    txHashBucketCache[txHashKey] = updatedBucket;

                    if (updatedBucket is null)
                        batch.Delete(txHashKey, _cfTxHash2TxNum);
                    else
                        batch.Put(txHashKey, updatedBucket, _cfTxHash2TxNum);
                }

                // Delete from txnum2txhash
                batch.Delete(txNumKey, _cfTxNum2TxHash);

                iterator.Next();
            }
        }

        // Delete blkinfo entries >= height
        using (var iterator = _db.NewIterator(_cfBlkinfo))
        {
            iterator.SeekToFirst();

            while (iterator.Valid())
            {
                var blkKey = iterator.Key();
                if (blkKey.Length == 4)
                {
                    var h = BinaryPrimitives.ReadInt32BigEndian(blkKey);
                    if (h >= height)
                        batch.Delete(blkKey, _cfBlkinfo);
                }

                iterator.Next();
            }
        }

        // Update next_txnum both in DB and in-memory cache
        SetNextTxNum(batch, firstTxNum);
        _nextTxNumInMemory = firstTxNum;
        InvalidateBlkinfoIndex();
    }

    /// <summary>
    ///     Converts a TxNum to a 6-byte little-endian buffer.
    /// </summary>
    /// <param name="txNum">Transaction number to convert.</param>
    /// <returns>6-byte buffer containing the encoded TxNum.</returns>
    private static byte[] TxNumToBytes(ulong txNum)
    {
        var buffer = new byte[TxNumCodec.Size];
        TxNumCodec.Encode(buffer.AsSpan(), txNum);
        return buffer;
    }

    /// <summary>
    ///     Converts a 6-byte little-endian buffer to a TxNum.
    /// </summary>
    /// <param name="bytes">Buffer containing the encoded TxNum.</param>
    /// <returns>Decoded transaction number.</returns>
    private static ulong TxNumFromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < TxNumCodec.Size)
            throw new ArgumentException($"TxNum requires {TxNumCodec.Size} bytes");

        return TxNumCodec.Decode(bytes);
    }

    /// <summary>
    ///     Extracts the last 6 bytes of a transaction hash for use as a lookup key.
    /// </summary>
    /// <param name="txHash">Transaction hash (32 bytes).</param>
    /// <returns>Last 6 bytes of the transaction hash.</returns>
    private static byte[] GetTxHashKey(byte[] txHash)
    {
        // Use last 6 bytes of TxHash (in big-endian order)
        var key = new byte[6];
        Array.Copy(txHash, txHash.Length - 6, key, 0, 6);
        return key;
    }

    /// <summary>
    ///     Gets the next TxNum to assign, either from memory cache or database.
    /// </summary>
    /// <returns>The next transaction number to assign.</returns>
    private ulong GetNextTxNum()
    {
        if (_batchActive)
            return _nextTxNumInMemory;

        _nextTxNumInMemory = GetNextTxNumFromDb();
        return _nextTxNumInMemory;
    }

    /// <summary>
    ///     Increments the next TxNum counter and writes it to the database.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    private void IncrementNextTxNum(WriteBatch batch)
    {
        _nextTxNumInMemory++;
        SetNextTxNum(batch, _nextTxNumInMemory);
    }

    /// <summary>
    ///     Reads the next TxNum counter from the database.
    /// </summary>
    /// <returns>The next transaction number to assign, or 0 if not set.</returns>
    private ulong GetNextTxNumFromDb()
    {
        var value = _db.Get(Encoding.UTF8.GetBytes("next_txnum"), _cfMetadata);
        if (value is null || value.Length < 8)
            return 0;

        return BinaryPrimitives.ReadUInt64LittleEndian(value);
    }

    /// <summary>
    ///     Writes the next TxNum counter to the database.
    /// </summary>
    /// <param name="batch">Write batch for atomic operations.</param>
    /// <param name="txNum">Transaction number to set as next.</param>
    private void SetNextTxNum(WriteBatch batch, ulong txNum)
    {
        var key = Encoding.UTF8.GetBytes("next_txnum");
        var value = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(value, txNum);
        batch.Put(key, value, _cfMetadata);
    }

    /// <summary>
    ///     Removes a TxNum from an encoded VarInt list. Returns null when the list becomes empty.
    /// </summary>
    private static byte[]? RemoveTxNumFromBucket(byte[] encodedList, ulong txNumToRemove)
    {
        var kept = new List<ulong>();
        var removed = false;
        var offset = 0;

        while (offset < encodedList.Length)
        {
            var (txNum, bytesRead) = VarInt.Decode(encodedList.AsSpan(offset));
            offset += bytesRead;

            if (txNum == txNumToRemove)
            {
                removed = true;
                continue;
            }

            kept.Add(txNum);
        }

        if (!removed)
            return encodedList;

        if (kept.Count == 0)
            return null;

        var buffer = new byte[kept.Count * 10];
        var position = 0;
        foreach (var txNum in kept)
            position += VarInt.Encode(txNum, buffer.AsSpan(position));

        return buffer[..position];
    }

    private List<(ulong firstTxNum, int height)> GetOrBuildBlkinfoIndex()
    {
        lock (_blkinfoIndexLock)
        {
            if (_blkinfoIndex is not null)
                return _blkinfoIndex;

            var index = new List<(ulong firstTxNum, int height)>();
            using var iterator = _db.NewIterator(_cfBlkinfo);
            iterator.SeekToFirst();
            while (iterator.Valid())
            {
                var key = iterator.Key();
                var value = iterator.Value();
                if (key.Length == 4 && value.Length >= TxNumCodec.Size)
                {
                    var height = BinaryPrimitives.ReadInt32BigEndian(key);
                    var firstTxNum = TxNumFromBytes(value.AsSpan(0, TxNumCodec.Size));
                    index.Add((firstTxNum, height));
                }

                iterator.Next();
            }

            index.Sort(static (a, b) => a.firstTxNum.CompareTo(b.firstTxNum));
            _blkinfoIndex = index;
            return _blkinfoIndex;
        }
    }

    private static int FindFirstBlockIndexForTxNum(List<(ulong firstTxNum, int height)> index, ulong txNum)
    {
        var low = 0;
        var high = index.Count - 1;
        var match = -1;

        while (low <= high)
        {
            var mid = low + (high - low) / 2;
            if (index[mid].firstTxNum <= txNum)
            {
                match = mid;
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        return match;
    }

    private void InvalidateBlkinfoIndex()
    {
        lock (_blkinfoIndexLock)
        {
            _blkinfoIndex = null;
        }
    }
}